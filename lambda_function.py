import json
import boto3
import base64
import gzip
import os
from datetime import datetime
from typing import List, Dict, Any

# Initialize the Firehose client
firehose_client = boto3.client('firehose')
DELIVERY_STREAM_NAME = os.environ.get('FIREHOSE_STREAM_NAME', 'iceberg-logs-stream')

def parse_log_message(message: str) -> Dict[str, Any]:
    """
    Parse a log message and extract structured fields.
    Handles both JSON-formatted logs and plain text logs.
    """
    try:
        # Attempt to parse as JSON
        if message.strip().startswith('{'):
            return json.loads(message)
        else:
            # For plain text logs, create a structured format
            return {
                'message': message,
                'log_level': 'INFO'  # Default level
            }
    except json.JSONDecodeError:
        # If JSON parsing fails, treat as plain text
        return {
            'message': message,
            'log_level': 'INFO'
        }

def transform_log_event(log_event: Dict[str, Any], log_group: str, log_stream: str) -> Dict[str, Any]:
    """
    Transform a CloudWatch log event into the target Iceberg table schema.
    """
    # Extract timestamp from the log event
    timestamp_ms = log_event.get('timestamp', 0)
    event_timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0).isoformat()
    
    # Parse the log message
    message_data = parse_log_message(log_event.get('message', ''))
    
    # Build the transformed record matching the Iceberg schema
    transformed_record = {
        'event_timestamp': event_timestamp,
        'log_level': message_data.get('log_level', 'INFO'),
        'message': message_data.get('message', ''),
        'application_name': message_data.get('application', log_group.split('/')[-1]),
        'user_id': message_data.get('user_id', ''),
        'session_id': message_data.get('session_id', ''),
        'processed_at': datetime.utcnow().isoformat()
    }
    
    return transformed_record

def send_to_firehose(records: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Send a batch of records to Kinesis Data Firehose.
    Returns a dictionary with success and failure counts.
    """
    if not records:
        return {'success': 0, 'failed': 0}
    
    # Prepare records for Firehose (max 500 per batch)
    firehose_records = []
    for record in records:
        # Convert to JSON and add newline
        data = json.dumps(record) + ''
        firehose_records.append({'Data': data.encode('utf-8')})
    
    success_count = 0
    failed_count = 0
    
    # Send in batches of 500 (Firehose limit)
    batch_size = 500
    for i in range(0, len(firehose_records), batch_size):
        batch = firehose_records[i:i + batch_size]
        
        try:
            response = firehose_client.put_record_batch(
                DeliveryStreamName=DELIVERY_STREAM_NAME,
                Records=batch
            )
            
            # Check for partial failures
            failed_put_count = response.get('FailedPutCount', 0)
            success_count += len(batch) - failed_put_count
            failed_count += failed_put_count
            
            # Log any failures for debugging
            if failed_put_count > 0:
                print(f"Batch had {failed_put_count} failed records")
                for idx, record_response in enumerate(response.get('RequestResponses', [])):
                    if 'ErrorCode' in record_response:
                        print(f"Record {idx} failed: {record_response.get('ErrorCode')} - {record_response.get('ErrorMessage')}")
        
        except Exception as e:
            print(f"Error sending batch to Firehose: {str(e)}")
            failed_count += len(batch)
    
    return {'success': success_count, 'failed': failed_count}

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function.
    Processes CloudWatch Logs events and sends them to Firehose.
    """
    print(f"Received event with {len(event.get('awslogs', {}))} log entries")
    
    try:
        # Decode and decompress the CloudWatch Logs data
        compressed_payload = base64.b64decode(event['awslogs']['data'])
        uncompressed_payload = gzip.decompress(compressed_payload)
        log_data = json.loads(uncompressed_payload)
        
        # Extract metadata
        log_group = log_data.get('logGroup', '')
        log_stream = log_data.get('logStream', '')
        log_events = log_data.get('logEvents', [])
        
        print(f"Processing {len(log_events)} log events from {log_group}/{log_stream}")
        
        # Transform all log events
        transformed_records = []
        for log_event in log_events:
            try:
                transformed_record = transform_log_event(log_event, log_group, log_stream)
                transformed_records.append(transformed_record)
            except Exception as e:
                print(f"Error transforming log event: {str(e)}")
                continue
        
        # Send to Firehose
        result = send_to_firehose(transformed_records)
        
        print(f"Processing complete. Success: {result['success']}, Failed: {result['failed']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'processed': len(log_events),
                'transformed': len(transformed_records),
                'success': result['success'],
                'failed': result['failed']
            })
        }
    
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

