#!/usr/bin/env python3
import boto3
import time
import json
import random
from datetime import datetime, timedelta

# Configuration - change the log group and log stream as needed
LOG_GROUP_NAME = '/aws/application/logs'
LOG_STREAM_NAME = 'test-stream'
NUM_EVENTS = 1000

# Sample data
CUSTOMER_NAMES = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve']

# Create CloudWatch Logs client
client = boto3.client('logs', region_name='us-east-1')

# Create log stream
try:
    client.create_log_stream(
        logGroupName=LOG_GROUP_NAME,
        logStreamName=LOG_STREAM_NAME
    )
    print(f"Created log stream: {LOG_STREAM_NAME}")
except client.exceptions.ResourceAlreadyExistsException:
    print(f"Log stream already exists: {LOG_STREAM_NAME}")

# Generate bulk events matching the Iceberg table schema:
# id (INT), customer_name (STRING), amount (INT), order_date (DATE)
log_events = []
for i in range(1, NUM_EVENTS + 1):
    timestamp = int(time.time() * 1000)
    order_date = (datetime.utcnow() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d')

    message = {
        "id": i,
        "customer_name": random.choice(CUSTOMER_NAMES),
        "amount": random.randint(50, 500),
        "order_date": order_date
    }

    log_events.append({
        'timestamp': timestamp,
        'message': json.dumps(message)
    })
    print(message)
    time.sleep(0.001)  # Small delay to ensure unique timestamps

# Send events in batches (max 10,000 events or 1MB per request)
BATCH_SIZE = 1000
for i in range(0, len(log_events), BATCH_SIZE):
    batch = log_events[i:i + BATCH_SIZE]
    try:
        response = client.put_log_events(
            logGroupName=LOG_GROUP_NAME,
            logStreamName=LOG_STREAM_NAME,
            logEvents=batch
        )
        print(f"Sent batch {i // BATCH_SIZE + 1}: {len(batch)} events")
        if 'rejectedLogEventsInfo' in response:
            print(f"Rejected events info: {response['rejectedLogEventsInfo']}")
    except Exception as e:
        print(f"Error sending batch: {e}")

print(f"Completed! Sent {NUM_EVENTS} events to {LOG_GROUP_NAME}/{LOG_STREAM_NAME}")
