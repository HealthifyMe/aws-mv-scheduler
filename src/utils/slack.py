import json
import os
import urllib.request
from dotenv import load_dotenv

load_dotenv()

def send_message_to_slack(message, thread_ts=None):
    """Send a message to Slack."""
    slack_url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.environ.get('SLACK_BOT_TOKEN')}" 
    }
    payload = {
        "channel": os.environ.get('SLACK_CHANNEL_ID'),
        "text": message
    }
    
    if thread_ts:
        payload["thread_ts"] = thread_ts
    
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(slack_url, data=data, headers=headers)
    urllib.request.urlopen(req) 