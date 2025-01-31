import os
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
import uuid
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

from ..db.connections import get_dynamodb_tables, get_redshift_connection
from ..utils.config import load_config, get_refresh_buffer
from ..utils.slack import send_message_to_slack

# Configure logging
log_dir = os.getenv('LOG_DIR', 'logs')
logging.basicConfig(
    filename=os.path.join(log_dir, 'processor.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Get DynamoDB tables
queue_table, history_table = get_dynamodb_tables()

# Redshift connection
def get_redshift_connection():
    return psycopg2.connect(
        host=os.environ.get('REDSHIFT_HOST'),
        dbname=os.environ.get('REDSHIFT_DB_NAME'),
        port=os.environ.get('REDSHIFT_PORT', 5439),
        user=os.environ.get('REDSHIFT_USER_NAME'),
        password=os.environ.get('REDSHIFT_PASSWORD'),
    )

def check_mv_existence(mv_name):
    """
    Check if a Materialized View exists in the Redshift cluster.
    """
    conn = None  # Initialize conn to None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()
        query = f"SELECT COUNT(*) from stv_mv_info where name = '{mv_name}';"
        cur.execute(query)
        result = cur.fetchone()
        return result[0] > 0
    except Exception as e:
        logging.error(f"Error checking existence of MV {mv_name}: {e}")
        return False
    finally:
        if conn:
            cur.close()
            conn.close()

def fetch_dependencies_from_redshift(mv_name):
    """
    Fetch direct dependencies of an MV from Redshift.
    """
    conn = None  # Initialize conn to None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()
        query = f"SELECT ref_name FROM STV_MV_DEPS WHERE name = '{mv_name}'"
        cur.execute(query)
        result = cur.fetchall()
        dependencies = [row[0] for row in result]
        return dependencies
    except Exception as e:
        logging.error(f"Error fetching dependencies for {mv_name}: {e}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()

def reschedule_mv(current_mv_name, message_list):
    """
    Refreshes dependent MVs after the current MV is refreshed and returns a list of refreshed MVs.
    """


    try:
        response = queue_table.query(
            KeyConditionExpression=Key('mv_name').eq(current_mv_name)
        )
        items = response.get('Items', [])
        if items:
            item = items[0]

            rescheduled_time = datetime.now(timezone.utc) + timedelta(minutes=30)
            # Update status of current MV to 'pending'
            queue_table.update_item(
                Key={'mv_name': current_mv_name},  # Assuming 'mv_name' is the primary key
                UpdateExpression="SET #status = :new_status, #scheduled_time = :new_scheduled_time",
                ExpressionAttributeNames={
                    "#status": "status",
                    "#scheduled_time": "scheduled_time"
                },
                ExpressionAttributeValues={
                    ":new_status": "pending",
                    ":new_scheduled_time": rescheduled_time.isoformat()
                }
            )
            message_list.append(current_mv_name)
    except Exception as e:
        logging.error(f"Error updating status of {current_mv_name}: {e}")
        return message_list

    # Find MVs with dependencies on the current MV
    response = queue_table.scan(
        FilterExpression="contains(dependencies, :c)",
        ExpressionAttributeValues={':c': current_mv_name}
    )
    if response:
        for item in response['Items']:
            mv_name = item['mv_name']
            time.sleep(5)
            reschedule_mv(mv_name, message_list)
    else:
        return message_list

def fetch_latest_completed_tasks(dependencies, dependency_ids, history_table):
    """
    Fetch the latest completed tasks for the given dependencies from the mv_history table.
    Only considers entries with a status of 'completed' and checks if they are within the buffer range.
    """
    completed_tasks = []
    for mv_name in dependencies:
        try:
            # Scan the history table for completed entries for the specified MV
            last_refresh_time, last_refresh_id = fetch_last_refresh_info(mv_name, history_table)
            config = load_config()
            buffer_minutes=get_refresh_buffer(mv_name, config)
            logging.info(f"Last refresh happened for {mv_name} in last {datetime.now(timezone.utc) - last_refresh_time} hours")
            # Check if the last refresh time is within the buffer range
            if datetime.now(timezone.utc) - last_refresh_time <= timedelta(minutes=buffer_minutes):
                if mv_name not in completed_tasks:
                    completed_tasks.append(mv_name)
            else:
                # If the last refreshed time is outside the buffer range
                # Check if mv_name not in mv_queue then add the mv_name to completed tasks
                if not check_mv_in_queue(mv_name):
                    if mv_name not in completed_tasks:
                        completed_tasks.append(mv_name)
                        logging.error(f"Skipping {mv_name} to avoid deadlock because the mv not in the buffer range and not present in mv_queue for processing")

            # Check if any dependency IDs are present in the mv_history table
            if dependency_ids:  # Only check if dependency_ids is provided
                for dep_id in dependency_ids:
                    dependency_response = history_table.scan(
                        FilterExpression=Attr('refresh_id').eq(dep_id)  # Assuming refresh_id is the key to check
                    )
                    if 'Items' in dependency_response and dependency_response['Items']:
                        # Assuming you want to check the first item for the mv_id
                        dependency_id = dependency_response['Items'][0].get('refresh_id')
                        if dependency_id == dep_id and mv_name not in completed_tasks:  # Append only if not already present
                            completed_tasks.append(mv_name)  # Add the MV name if the dependency ID is present 

        except Exception as e:
            logging.error(f"Error fetching completed tasks for {mv_name}: {e}")

    return completed_tasks

def archive_mv_to_history(mv_name,mv_id, status, reason=None, refresh_time=None):
    """
    Archive the MV to the mv_history table with the specified status and reason.
    If reason is not provided, it defaults to None.
    If refresh_time is provided, it will be included in the history entry.
    """
    try:
        # If a failure reason is provided, use it as the reason
        if status == "failed" and reason is None:
            reason = "Failure Reason: Data validation error."

        # Always set last_refreshed_time to the current time
        last_refreshed_time = datetime.now(timezone.utc).isoformat()

        # Prepare the item to be inserted into the history table
        history_item = {
            'mv_name': mv_name,
            'status': status,
            'reason': reason if reason else None,  # Ensure reason is set
            'last_refreshed': last_refreshed_time,  # Always current time
            'refresh_id': mv_id  # Use the mv_id as refresh_id
        }

        # Add refresh_time to the item if it is provided
        if refresh_time:
            history_item['refresh_time'] = str(refresh_time)  # Convert refresh_time to string

        # Ensure all values are stored as strings
        for key in history_item:
            if isinstance(history_item[key], float):
                history_item[key] = str(history_item[key])  # Convert float to string
        # Store the item in the history table
        history_table.put_item(Item=history_item)
        logging.info(f"Archived {mv_name} to history with status '{status}' and reason '{history_item['reason']}'.")

        # Remove the MV from the queue table
        queue_table.delete_item(Key={'mv_name': mv_name})  # Assuming 'mv_name' is the primary key
        logging.info(f"Removed {mv_name} from the queue.")

    except Exception as e:
        logging.error(f"Error archiving {mv_name} to history: {e}")

def refresh_mv(mv_name, mv_id):
    """
    Refresh a Materialized View in Redshift and update its status to 'in progress'.
    Implements a retry mechanism for failed refresh attempts with increasing wait times.
    """
    # Change the status of the MV to 'in progress'
    queue_table.update_item(
        Key={'mv_name': mv_name},
        UpdateExpression="SET #status = :in_progress",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":in_progress": "in progress"}
    )
    logging.info(f"MV {mv_name} status updated to 'in progress'.")

    max_retries = 3 # Maximum number of retry attempts if the mv refresh fails
    attempt = 0
    success = False

    while attempt < max_retries and not success:
        conn = None  # Initialize conn to None
        try:
            conn = get_redshift_connection()  # Establish connection
            cur = conn.cursor()
            start_time = time.time()
            time.sleep(30)
            # Execute the refresh command
            cur.execute(f"REFRESH MATERIALIZED VIEW {mv_name};")
            conn.commit()  # Commit the transaction

            refresh_time = round(time.time() - start_time, 2)
            logging.info(f"{mv_name} refresh completed in {refresh_time} seconds.")
            success = True  # Mark success if refresh completes without exception
            archive_mv_to_history(mv_name, mv_id, 'completed', None, refresh_time)

        except Exception as e:
            attempt += 1
            logging.error(f"Error refreshing {mv_name}: {e}. Attempt {attempt} of {max_retries}.")
            if attempt < max_retries:
                wait_time = attempt * 2  # Wait time in minutes: 2, 4, 6 for attempts 1, 2, 3
                logging.info(f"Retrying in {wait_time} minutes...")
                time.sleep(wait_time * 60)  # Convert minutes to seconds for sleep
            else:
                message = f"Failed to refresh {mv_name} after {max_retries} attempts."
                send_message_to_slack(message)
                logging.error(f"{message}")
                message_list = []
                reschedule_mv(mv_name, message_list)
                slack_message = "Recheduled " + str(message_list) + " 30 mins from now"
                send_message_to_slack(slack_message)
        
        finally:
            if conn:
                cur.close()
                conn.close()

def fetch_oldest_eligible_task(queue_table):
    """
    Fetch the oldest eligible task from the mv_queue.
    """
    eligible_response = queue_table.scan(
        FilterExpression="#status = :eligible",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":eligible": "eligible"}
    )
    
    eligible_tasks = eligible_response.get('Items', [])
    
    # Sort eligible tasks by scheduled time and return the oldest one
    if eligible_tasks:
        eligible_tasks.sort(key=lambda x: x['scheduled_time'])  # Sort by scheduled time
        return eligible_tasks[0]  # Return the oldest eligible task
    return None  # Return None if no eligible tasks are found

def fetch_oldest_pending_task(queue_table):
    """
    Fetch the oldest pending task from the mv_queue.
    """
    pending_response = queue_table.scan(
        FilterExpression="#status = :pending",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":pending": "pending"}
    )
    
    pending_tasks = pending_response.get('Items', [])
    
    # Sort pending tasks by scheduled time and return the oldest one
    if pending_tasks:
        pending_tasks.sort(key=lambda x: x['scheduled_time'])  # Sort by scheduled time
        return pending_tasks[0]  # Return the oldest pending task
    return None  # Return None if no pending tasks are found

def process_eligible_task(task_to_process, history_table):
    """
    Process an eligible task.
    """
    mv_name = task_to_process['mv_name']
    mv_id = task_to_process['mv_id']

    # Check last refresh time and refresh_id in mv_history
    last_refresh_time, refresh_id = fetch_last_refresh_info(mv_name, history_table)

    if last_refresh_time:
        config = load_config()
        buffer_minutes=get_refresh_buffer(mv_name, config)
        # If the last refresh time is within the last 15 minutes, skip the task
        if (datetime.now(timezone.utc) - last_refresh_time) < timedelta(minutes=buffer_minutes):
            skip_reason = f"Skipped due to recent refresh (ID: {refresh_id})"
            archive_mv_to_history(mv_name, mv_id, "skipped", skip_reason)  # Use archive_mv_to_history instead
            logging.warning(f"{mv_name} skipped: {skip_reason}")
            return

    # Always set last_refreshed_time to the current time
    last_refreshed_time = datetime.now(timezone.utc).isoformat()

    # Proceed to refresh the MV since it is eligible
    refresh_mv(mv_name, mv_id)  # Refresh the MV
    logging.info(f"Processed MV: {mv_name}")

def get_mv_details(mv_name, queue_table):
    """
    Retrieve the details of a materialized view from the mv_queue table based on the mv_name.
    
    Parameters:
    mv_name (str): The name of the materialized view to retrieve.
    queue_table: The DynamoDB table resource for mv_queue.

    Returns:
    dict: The details of the materialized view if found, otherwise None.
    """
    try:
        response = queue_table.get_item(Key={'mv_name': mv_name})  # Assuming 'mv_name' is the partition key
        if 'Item' in response:
            logging.info(f"{response['Item']}")
            return response['Item']  # Return the details of the MV
        else:
            logging.warning(f"No entry found for MV: {mv_name} in mv_queue.")
            return None  # Return None if the MV is not found
    except Exception as e:
        logging.error(f"Error retrieving MV details for {mv_name}: {e}")
        return None  # Return None in case of an error

def process_pending_task(task_to_process, history_table):
    """
    Process a pending task.
    """
    mv_name = task_to_process['mv_name']
    mv_id = task_to_process['mv_id']

    # Check last refresh time and refresh_id in mv_history
    last_refresh_time, refresh_id = fetch_last_refresh_info(mv_name, history_table)

    if last_refresh_time:
        config = load_config()
        buffer_minutes = get_refresh_buffer(mv_name, config)
        logging.info(f"Last refresh happened for {mv_name} in last {datetime.now(timezone.utc) - last_refresh_time} hours")
        # If the last refresh time is within the buffer, skip the task
        if (datetime.now(timezone.utc) - last_refresh_time) < timedelta(minutes=buffer_minutes):
            skip_reason = f"Skipped due to recent refresh (ID: {refresh_id})"
            archive_mv_to_history(mv_name, mv_id, "skipped", skip_reason)
            logging.warning(f"{mv_name} skipped: {skip_reason}")
            return

    # Log the current task and its dependencies
    dependencies = task_to_process.get('dependencies', [])
    dependency_ids = task_to_process.get('dependency_ids', [])
    logging.info(f"Checking task: {mv_name}, Dependencies: {dependencies}")

    # Check for pending dependencies
    pending_dependencies = set(dependencies) - set(fetch_latest_completed_tasks(dependencies, dependency_ids, history_table))
    
    # Check if any dependencies are in progress
    if any(check_mv_in_progress(dep) for dep in dependencies):
        logging.info(f"Pending dependencies for {mv_name} are in progress. Fetching the next oldest pending task...")
        # Fetch the next oldest pending task that is scheduled after the current task
        next_pending_task = fetch_next_pending_task_after(queue_table, task_to_process)
        
        if next_pending_task:
            logging.info(f"Processing next pending task: {next_pending_task['mv_name']}")
            process_pending_task(next_pending_task, history_table)  # Process the newly fetched pending task
        else:
            logging.info(f"No next pending task found after {mv_name}.")
    else:
        if not pending_dependencies:  # If there are no pending dependencies
            mark_as_eligible(mv_name)  # Mark the MV as eligible instead of refreshing
            logging.info(f"Marked {mv_name} as eligible.")
        else:
            logging.info(f"Pending dependencies for {mv_name}: {list(pending_dependencies)}")  # Show only pending dependencies
            # Process each pending dependency
            for dependent_mv in pending_dependencies:
                logging.info(f"Processing dependency: {dependent_mv} for MV: {mv_name}")
                logging.info(f"QUEUE: {queue_table}")
                mv_pending = get_mv_details(dependent_mv, queue_table)
                if mv_pending:
                    # Call the function to process the current dependency
                    process_pending_task(mv_pending, history_table)  # You can replace this with your actual processing logic

def mark_as_eligible(mv_name):
    """
    Mark the MV as eligible for processing in the queue table.
    """
    try:
        # Update the status of the MV to 'eligible' in the queue table
        queue_table.update_item(
            Key={'mv_name': mv_name},  # Assuming 'mv_name' is the primary key
            UpdateExpression="SET #status = :eligible",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":eligible": "eligible"}
        )
        logging.info(f"{mv_name} has been marked as eligible for processing.")
    except Exception as e:
        logging.error(f"Error marking {mv_name} as eligible: {e}")

def fetch_last_refresh_info(mv_name, history_table):
    """
    Check the last refresh time and refresh_id of the MV from the mv_history table.
    Only considers entries with a status of 'completed'.
    """
    try:
        # Query the table to find all 'completed' items for the given mv_name
        response = history_table.query(
            KeyConditionExpression=Key('mv_name').eq(mv_name),
            FilterExpression=Attr('status').eq('completed')
        )

        items = response.get('Items', [])
        if items:
            items = sorted(items, key=lambda x: x['last_refreshed'], reverse=True)
            last_item = items[0]
            last_refreshed = last_item['last_refreshed']
            refresh_id = last_item['refresh_id']
            logging.info(f"{mv_name} refreshed at {datetime.fromisoformat(last_refreshed)}")
            return datetime.fromisoformat(last_refreshed), refresh_id  # Return both values
        else:
            logging.warning(f"No completed item found for MV: {mv_name} in history.")
    except Exception as e:
        logging.error(f"Error fetching last refresh time for {mv_name}: {e}")
    
    return None, None  # Return None for both if not found

def check_mv_in_queue(mv_name):
    """
    Check if there is any entry for the given mv_name in the mv_queue table.

    Parameters:
    mv_name (str): The name of the materialized view to check.

    Returns:
    bool: True if an entry exists, False otherwise.
    """
    try:
        response = queue_table.get_item(Key={'mv_name': mv_name})  # Assuming 'mv_name' is the primary key
        return 'Item' in response  # Return True if the item exists, False otherwise
    except Exception as e:
        logging.error(f"Error checking mv_name {mv_name} in mv_queue: {e}")
        return False  # Return False in case of an error

def fetch_next_pending_task_after(queue_table, current_task):
    """
    Fetch the next oldest pending task that is scheduled after the current task.

    Parameters:
    queue_table: The DynamoDB table resource for mv_queue.
    current_task: The current task being processed.

    Returns:
    dict: The next pending task if found, otherwise None.
    """
    try:
        # Assuming 'scheduled_time' is a field in the queue that indicates when the task is scheduled
        response = queue_table.scan(
            FilterExpression="#status = :pending AND scheduled_time > :current_time",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":pending": "pending",
                ":current_time": current_task['scheduled_time']  # Use the scheduled time of the current task
            }
        )
        
        pending_tasks = response.get('Items', [])
        
        # Sort pending tasks by scheduled time and return the oldest one
        if pending_tasks:
            pending_tasks.sort(key=lambda x: x['scheduled_time'])  # Sort by scheduled time
            return pending_tasks[0]  # Return the oldest pending task
        return None  # Return None if no pending tasks are found
    except Exception as e:
        logging.error(f"Error fetching next pending task after {current_task['mv_name']}: {e}")
        return None

def check_mv_in_progress(mv_name):
    """
    Check if the given mv_name is in progress in the mv_queue table.

    Parameters:
    mv_name (str): The name of the materialized view to check.

    Returns:
    bool: True if the MV is in progress, False otherwise.
    """
    try:
        response = queue_table.get_item(Key={'mv_name': mv_name})  # Assuming 'mv_name' is the primary key
        if 'Item' in response:
            return response['Item'].get('status') == 'in progress'  # Check if status is 'in progress'
        return False  # Return False if the item does not exist
    except Exception as e:
        logging.error(f"Error checking mv_name {mv_name} in mv_queue: {e}")
        return False  # Return False in case of an error

def remove_null_scheduled_time_records(queue_table):
    """
    Check for records in the mv_queue with scheduled_time as null and remove them.

    Parameters:
    queue_table: The DynamoDB table resource for mv_queue.
    """
    try:
        # Scan the queue_table for records with scheduled_time as null
        response = queue_table.scan(
        FilterExpression=Attr('scheduled_time').not_exists() | Attr('scheduled_time').eq(None)  # Check for null or non-existent
        )
        
        items_to_delete = response.get('Items', [])
        if items_to_delete:
            for item in items_to_delete:
                # Remove the item from the queue_table
                queue_table.delete_item(
                    Key={'mv_name': item['mv_name']}  # Assuming 'mv_name' is the primary key
                )
                logging.error(f"Removed item with mv_name: {item['mv_name']} from mv_queue due to null scheduled_time.")

    except Exception as e:
        logging.error(f"Error removing records with null scheduled_time: {e}")

def master_worker():
    """
    Master function that processes MVs in the queue and marks them as eligible.
    """
    try:
        while True:
            #remove_null_scheduled_time_records(queue_table)
            task_to_process = fetch_oldest_pending_task(queue_table)

            # Debug: Log the oldest pending task
            if task_to_process:
                # Check if scheduled_time is null
                if task_to_process.get('scheduled_time') is None:
                    # Archive the task to mv_history with status 'skipped'
                    archive_mv_to_history(
                        mv_name=task_to_process['mv_name'],
                        mv_id=task_to_process['mv_id'],
                        status='skipped',
                        reason='scheduled_time is null'
                    )
                    logging.info(f"Moved task {task_to_process['mv_name']} to mv_history with status 'skipped' due to null scheduled_time.")
                else:
                    logging.info(f"Processing oldest pending task: {task_to_process['mv_name']}")
                    process_pending_task(task_to_process, history_table)
            time.sleep(30)
    except Exception as e:
        logging.error(f"Error in master worker: {e}")

def worker(worker_id):
    """
    Worker function that continuously processes the queue for MVs to refresh.
    """
    while True:
        try:
            time.sleep(60 * (worker_id - 1))
            remove_null_scheduled_time_records(queue_table)
            # Fetch the oldest eligible task from the queue_table
            task_to_process = fetch_oldest_eligible_task(queue_table)

            if task_to_process:
                    # Check if scheduled_time is null
                    if task_to_process.get('scheduled_time') is None:
                        # Archive the task to mv_history with status 'skipped'
                        archive_mv_to_history(
                            mv_name=task_to_process['mv_name'],
                            mv_id=task_to_process['mv_id'],
                            status='skipped',
                            reason='scheduled_time is null'
                        )
                        logging.info(f"Moved task {task_to_process['mv_name']} to mv_history with status 'skipped' due to null scheduled_time.")
                    else:
                        logging.info(f"Worker {worker_id} - Processing eligible task: {task_to_process['mv_name']}")
                        process_eligible_task(task_to_process, history_table)
            else:
                time.sleep(5)  # Sleep for a while if no tasks are found

        except Exception as e:
            logging.error(f"Worker {worker_id} encountered an error: {e}")

def start_workers(num_workers=2):
    """
    Start the specified number of worker threads.
    
    Parameters:
    num_workers (int): The number of worker threads to start.
    """
    threads = []
    for i in range(num_workers):
        thread = threading.Thread(target=worker, args=(i + 1,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()  # Wait for all threads to finish (they won't in this infinite loop)

if __name__ == "__main__":
    # Start the master worker in a separate thread
    master_thread = threading.Thread(target=master_worker)
    master_thread.start()
    
    start_workers()  # Start the workers when the script is run
