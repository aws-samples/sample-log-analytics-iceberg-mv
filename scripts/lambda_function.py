import json
import boto3
import base64
import gzip
import os
from typing import List, Dict, Any

# Initialize the Firehose client
firehose_client = boto3.client('firehose')
DELIVERY_STREAM_NAME = os.environ.get('FIREHOSE_STREAM_NAME', 'iceberg-logs-stream')


def parse_log_message(message: str) -> Dict[str, Any]:
    """Parse a log message and extract structured fields.

    Expects JSON with: id, customer_name, amount, order_date
    """
    try:
        return json.loads(message)
    except json.JSONDecodeError:
        return None


def send_to_firehose(records: List[Dict[str, Any]]) -> Dict[str, int]:
    """Send a batch of records to Kinesis Data Firehose.

    Returns a dictionary with success and failure counts.
    """
    if not records:
        return {'success': 0, 'failed': 0}

    # Prepare records for Firehose (max 500 per batch)
    firehose_records = []
    for record in records:
        data = json.dumps(record) + '\n'
        firehose_records.append({'Data': data.encode('utf-8')})

    success_count = 0
    failed_count = 0

    # Send in batches of 500 (Firehose limit)
    batch_size = 500
    for i in range(0, len(firehose_records), batch_size):
        batch = firehose_records[i:i + batch_size]
        try:
            response = firehose_client.put_record_batch(
                DeliveryStreamName=DELIVERY_STREAM_NAME, Records=batch
            )

            failed_put_count = response.get('FailedPutCount', 0)
            success_count += len(batch) - failed_put_count
            failed_count += failed_put_count

            if failed_put_count > 0:
                print(f"Batch had {failed_put_count} failed records")
                for idx, record_response in enumerate(
                    response.get('RequestResponses', [])
                ):
                    if 'ErrorCode' in record_response:
                        print(
                            f"Record {idx} failed: "
                            f"{record_response.get('ErrorCode')} - "
                            f"{record_response.get('ErrorMessage')}"
                        )
        except Exception as e:
            print(f"Error sending batch to Firehose: {str(e)}")
            failed_count += len(batch)

    return {'success': success_count, 'failed': failed_count}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler function.

    Processes CloudWatch Logs events and forwards records to Firehose.
    Expects log messages with: id, customer_name, amount, order_date
    """
    try:
        # Decode and decompress the CloudWatch Logs data
        compressed_payload = base64.b64decode(event['awslogs']['data'])
        uncompressed_payload = gzip.decompress(compressed_payload)
        log_data = json.loads(uncompressed_payload)

        # Extract metadata
        log_group = log_data.get('logGroup', '')
        log_stream = log_data.get('logStream', '')
        log_events = log_data.get('logEvents', [])

        print(
            f"Processing {len(log_events)} log events "
            f"from {log_group}/{log_stream}"
        )

        # Parse and forward records matching the Iceberg schema
        records = []
        skipped = 0
        for log_event in log_events:
            parsed = parse_log_message(log_event.get('message', ''))
            if parsed and 'id' in parsed:
                # Forward only the Iceberg table columns
                records.append({
                    'id': parsed['id'],
                    'customer_name': parsed.get('customer_name', ''),
                    'amount': parsed.get('amount', 0),
                    'order_date': parsed.get('order_date', ''),
                })
            else:
                skipped += 1

        if skipped > 0:
            print(f"Skipped {skipped} non-matching log events")

        # Send to Firehose
        result = send_to_firehose(records)

        print(
            f"Processing complete. "
            f"Forwarded: {result['success']}, Failed: {result['failed']}"
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'processed': len(log_events),
                'forwarded': result['success'],
                'failed': result['failed'],
                'skipped': skipped,
            }),
        }

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
