import os
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
import uuid
from decimal import Decimal

from ..db.connections import get_dynamodb_tables, get_redshift_connection
from ..utils.config import load_config, get_refresh_buffer
from ..utils.slack import send_message_to_slack

# Configure logging
logging.basicConfig(
    filename='logs/processor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Get DynamoDB tables
queue_table, history_table = get_dynamodb_tables()

# Rest of the processor.py code remains the same, but remove the duplicate functions
# that were moved to other modules 