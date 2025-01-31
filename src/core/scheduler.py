import os
import logging
import time
import uuid
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from croniter import croniter

from ..db.connections import get_dynamodb_tables, get_redshift_connection
from ..utils.config import load_mv_config

# Configure logging
log_dir = os.getenv('LOG_DIR', 'logs')
logging.basicConfig(
    filename=os.path.join(log_dir, 'scheduler.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Get DynamoDB tables
queue_table, _ = get_dynamodb_tables()

# Redshift connection
def get_redshift_connection():
    return get_redshift_connection()

def check_mv_existence(mv_name):
    """
    Check if a Materialized View exists in either the YAML configuration or Redshift stv_mv_info table.
    :param mv_name: The name of the MV to check.
    :return: True if the MV exists, False otherwise.
    """
    # Check if the MV is defined in the configuration
    try:
        config = load_mv_config()
        mvs_config = {mv['name']: mv for mv in config['mvs']}
        if mv_name in mvs_config:
            return True
    except Exception as e:
        logging.error(f"Error checking MV {mv_name} in configuration: {e}")

    # Fallback: Check if the MV exists in Redshift
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()

        query = f"SELECT 1 FROM stv_mv_info WHERE name = '{mv_name.strip()}'"
        cur.execute(query)
        result = cur.fetchone()

        if result:
            return True
        else:
            logging.warning(f"MV {mv_name} does not exist in Redshift.")
            return False
    except Exception as e:
        logging.error(f"Error checking MV {mv_name} in Redshift: {e}")
        return False
    finally:
        if conn:
            cur.close()
            conn.close()

    return False

def get_next_scheduled_time(cron_expr, from_time=None):
    """
    Compute the next scheduled time based on the cron expression.
    """
    if from_time is None:
        from_time = datetime.now(timezone.utc)
    cron = croniter(cron_expr, from_time)
    return cron.get_next(datetime)

def topological_sort(dependency_graph):
    """
    Perform topological sorting on the dependency graph.
    """
    in_degree = defaultdict(int)

    for mv, deps in dependency_graph.items():
        for dep in deps:
            in_degree[dep] += 1

    # Include nodes with zero dependencies
    for mv in dependency_graph:
        if mv not in in_degree:
            in_degree[mv] = 0

    logging.info(f"Initial in-degrees: {dict(in_degree)}")

    zero_in_degree = deque([mv for mv in dependency_graph if in_degree[mv] == 0])
    sorted_order = []

    while zero_in_degree:
        mv = zero_in_degree.popleft()
        sorted_order.append(mv)

        for dep in dependency_graph[mv]:
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                zero_in_degree.append(dep)

    remaining_nodes = {mv: deg for mv, deg in in_degree.items() if deg > 0}
    if remaining_nodes:
        logging.info(f"Remaining nodes with in-degrees: {remaining_nodes}")
        raise ValueError("Circular dependency detected in the dependency graph.")

    return sorted_order

def build_dependency_graph(mvs_config):
    """
    Build a complete dependency graph for the given MVs.
    Ensure all nodes are included, even those without dependencies.
    """
    dependency_graph = defaultdict(list)

    for mv_name in mvs_config.keys():
        mv_name = mv_name.strip()
        dependencies = collect_all_dependencies(mv_name)
        dependency_graph[mv_name] = [dep.strip() for dep in dependencies]

        for dep in dependencies:
            if dep.strip() not in dependency_graph:
                dependency_graph[dep.strip()] = []

    # Debug: Print the dependency graph
    logging.info(f"Dependency Graph: {dependency_graph}")
    return dependency_graph

def fetch_dependencies_from_redshift(mv_name):
    """
    Fetch direct dependencies of a Materialized View from Redshift.
    :param mv_name: The name of the MV whose dependencies are to be fetched.
    :return: A list of dependent MV names with whitespace trimmed.
    """
    conn = None  # Initialize conn to None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()

        # Query the Redshift system table for dependencies
        query = f"SELECT ref_name FROM STV_MV_DEPS WHERE name = '{mv_name.strip()}'"
        cur.execute(query)
        result = cur.fetchall()

        # Trim whitespace from all dependency names
        dependencies = [row[0].strip() for row in result]
        logging.info(f"Fetched dependencies for {mv_name.strip()}: {dependencies}")
        return dependencies
    except Exception as e:
        logging.error(f"Error fetching dependencies for {mv_name.strip()}: {e}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()

def collect_all_dependencies(mv_name, visited=None):
    """
    Recursively collect all dependencies for a given MV.
    :param mv_name: The name of the MV to process.
    :param visited: A set of already visited MVs to prevent duplication.
    :return: A list of all dependent MVs.
    """
    if visited is None:
        visited = set()

    if mv_name in visited:
        return []

    visited.add(mv_name)

    # Fetch direct dependencies
    dependencies = [dep.strip() for dep in fetch_dependencies_from_redshift(mv_name)]
    all_dependencies = set(dependencies)

    # Recursively fetch dependencies for each dependency
    for dep in dependencies:
        all_dependencies.update(collect_all_dependencies(dep, visited))

    return list(all_dependencies)

def schedule_mv(mv_name, mvs_config, latest_scheduled_time):
    """
    Recursively schedule the materialized view and its dependencies.
    """
    def schedule_dependencies(mv_name, latest_scheduled_time):
        # Fetch dependencies for the current MV
        dependencies = fetch_dependencies_from_redshift(mv_name)
        dependency_ids = []

        # Schedule each dependency
        for dep in dependencies:
            # Recursively schedule the dependency
            dep_latest_time = schedule_dependencies(dep, latest_scheduled_time)

            # Fetch the mv_id for the dependency
            mv_id_response = queue_table.get_item(Key={'mv_name': dep})
            if 'Item' in mv_id_response:
                dependency_ids.append(mv_id_response['Item']['mv_id'])  # Get the mv_id of the dependency

        # Schedule the current MV
        mv_id = str(uuid.uuid4())  # Generate a UUID for the current MV
        status = 'pending'  # Set the status to 'pending'
        next_schedule_time = datetime.now(timezone.utc)  # Set the scheduled time to the current time
        scheduled_time = next_schedule_time.isoformat()
        # Validate required fields
        if not all([mv_id, mv_name, status, scheduled_time]):
            error_message = "Error: One or more required fields are null: mv_id, mv_name, status, scheduled_time."
            logging.error(error_message)
            raise ValueError(error_message)  # Raise an error to prevent insertion

        logging.info(f"Adding MV {mv_name} to DynamoDB...")
        queue_table.put_item(Item={
            'mv_id': mv_id,
            'mv_name': mv_name,
            'status': status,
            'scheduled_time': scheduled_time,
            'dependencies': dependencies,
            'dependency_ids': dependency_ids
        })
        logging.info(f"Scheduled MV {mv_name} for refresh at {next_schedule_time}.")
        # Update the latest scheduled time to ensure uniqueness
        return max(latest_scheduled_time, next_schedule_time)

    # Start the recursive scheduling
    latest_scheduled_time = schedule_dependencies(mv_name, latest_scheduled_time)

    return latest_scheduled_time

def schedule_tasks():
    """
    Schedule MVs in the correct execution order based on their dependencies.
    Dynamically discovered dependencies are scheduled based on dependency order.
    """
    config = load_mv_config()
    if not config:
        return

    mvs_config = {mv['name']: mv for mv in config['mvs']}
    due_mvs = {}

    # Check which MVs are due
    for mv_name, mv_config in mvs_config.items():
        if not check_mv_existence(mv_name):
            logging.warning(f"WARNING: MV {mv_name} does not exist. Skipping...")
            continue

        # Calculate next schedule for base MVs
        frequency = mv_config['frequency']
        response = queue_table.get_item(Key={'mv_name': mv_name})
        item = response.get('Item', {})
        # Check if 'scheduled_time' is a string before converting
        last_scheduled_time = (
            datetime.fromisoformat(item.get('scheduled_time')) 
            if isinstance(item.get('scheduled_time'), str) 
            else datetime.now(timezone.utc)
        )
        next_schedule = get_next_scheduled_time(frequency, last_scheduled_time)

        # Check if the task is due
        current_time = datetime.now(timezone.utc)
        if current_time + timedelta(minutes=1) >= next_schedule:
            # Check if the MV is already in the queue
            existing_mv_response = queue_table.get_item(Key={'mv_name': mv_name})
            if 'Item' in existing_mv_response:
                logging.info(f"DEBUG: MV {mv_name} is already scheduled. Skipping...")
                continue  # Skip scheduling if the MV is already in the queue

            due_mvs[mv_name] = mv_config
            logging.info(f"MV {mv_name} is due for refresh at {next_schedule}.")

    # Schedule tasks in reverse order
    for mv_name in due_mvs.keys():
        latest_scheduled_time = datetime.now(timezone.utc)
        latest_scheduled_time = schedule_mv(mv_name, mvs_config, latest_scheduled_time)

if __name__ == "__main__":
    while True:
        logging.info("Checking for tasks to schedule...")
        schedule_tasks()
        time.sleep(30)  # Run every 30 seconds
