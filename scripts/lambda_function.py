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


def process_cloudwatch_logs(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process a CloudWatch Logs event."""
    compressed_payload = base64.b64decode(event['awslogs']['data'])
    uncompressed_payload = gzip.decompress(compressed_payload)
    log_data = json.loads(uncompressed_payload)

    log_group = log_data.get('logGroup', '')
    log_stream = log_data.get('logStream', '')
    log_events = log_data.get('logEvents', [])

    print(
        f"Processing {len(log_events)} log events "
        f"from {log_group}/{log_stream}"
    )

    records = []
    skipped = 0
    for log_event in log_events:
        parsed = parse_log_message(log_event.get('message', ''))
        if parsed and 'id' in parsed:
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

    result = send_to_firehose(records)

    return {
        'processed': len(log_events),
        'forwarded': result['success'],
        'failed': result['failed'],
        'skipped': skipped,
    }


def process_sqs_records(event: Dict[str, Any]) -> Dict[str, Any]:
    """Reprocess failed events from the DLQ (SQS trigger)."""
    sqs_records = event.get('Records', [])
    print(f"Reprocessing {len(sqs_records)} events from DLQ")

    total_forwarded = 0
    total_failed = 0

    for sqs_record in sqs_records:
        try:
            # The SQS message body contains the original CloudWatch Logs event
            original_event = json.loads(sqs_record['body'])
            result = process_cloudwatch_logs(original_event)
            total_forwarded += result['forwarded']
            total_failed += result['failed']
        except Exception as e:
            print(f"Error reprocessing DLQ record: {str(e)}")
            total_failed += 1

    return {
        'reprocessed': len(sqs_records),
        'forwarded': total_forwarded,
        'failed': total_failed,
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler function.

    Handles two event sources:
    - CloudWatch Logs (subscription filter): event contains 'awslogs' key
    - SQS (DLQ reprocessing): event contains 'Records' key
    """
    try:
        if 'awslogs' in event:
            result = process_cloudwatch_logs(event)
        elif 'Records' in event:
            result = process_sqs_records(event)
        else:
            print(f"Unknown event format: {json.dumps(event)[:200]}")
            return {'statusCode': 400, 'body': json.dumps({'error': 'Unknown event format'})}

        print(f"Processing complete: {result}")
        return {'statusCode': 200, 'body': json.dumps(result)}

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        raise  # Re-raise so the event goes to DLQ
