import json
import boto3
import time
import random
from datetime import datetime, timezone
from botocore.config import Config

my_config = Config(
    region_name='eu-central-1'
)

# Initialize Firehose client
firehose_client = boto3.client('firehose', config=my_config)

# Enhanced log data structures
SERVICES = {
    'checkout': ['checkout_started', 'checkout_completed', 'checkout_failed'],
    'order': ['order_created', 'order_failed', 'order_cancelled'],
    'search': ['search_results', 'autosuggest', 'search_failed'],
    'cart': ['add_to_cart', 'remove_from_cart', 'cart_expired'],
    'payment': ['payment_authorized', 'payment_failed', 'payment_retry'],
    'auth': ['login_success', 'login_fail', 'password_reset_requested'],
    'shipment': ['label_printed', 'shipment_dispatched', 'delivery_failed'],
    'catalog': ['product_view', 'exception']
}

ERROR_SCENARIOS = {
    'payment': {
        'insufficient_funds': 'Payment declined due to insufficient funds',
        'rate_limit': 'Payment provider rate limit exceeded',
        'invalid_card': 'Invalid card information provided'
    },
    'auth': {
        'invalid_credentials': 'Invalid username or password',
        'account_locked': 'Account locked due to multiple failed attempts',
        'session_expired': 'User session expired'
    },
    'catalog': {
        'database_error': 'Database connection timeout',
        'invalid_sku': 'Product SKU not found in catalog',
        'stock_error': 'Failed to fetch stock information'
    }
}

def generate_log_entry():
    """Generate a realistic log entry with errors"""
    service = random.choice(list(SERVICES.keys()))
    level = random.choices(['INFO', 'WARN', 'ERROR', 'DEBUG'], weights=[40, 30, 20, 10])[0]

    log_entry = {
        'ts': datetime.now(timezone.utc).isoformat(),
        'lvl': level,
        'service': service,
        'event': random.choice(SERVICES[service]),
        'request_id': f'r-{random.randint(1000, 9999):04x}',
        'user_id': f'u-{random.randint(1000, 1999)}',
        'ip': f'192.0.{random.randint(1, 255)}.{random.randint(1, 255)}'
    }

    # Add error details for ERROR level logs
    if level == 'ERROR':
        if service in ERROR_SCENARIOS:
            error_type = random.choice(list(ERROR_SCENARIOS[service].keys()))
            log_entry['error'] = {
                'type': error_type,
                'message': ERROR_SCENARIOS[service][error_type],
                'error_code': f'E{random.randint(1000, 9999)}'
            }

    # Add service-specific fields
    if service == 'payment':
        log_entry['amount'] = round(random.uniform(10, 1000), 2)
        log_entry['currency'] = 'USD'
        if level == 'WARN':
            log_entry['attempt'] = random.randint(1, 3)
    elif service == 'cart':
        log_entry['cart_value'] = round(random.uniform(50, 500), 2)
        log_entry['sku'] = f'SKU-{random.randint(100000, 999999)}'
    elif service == 'order':
        log_entry['order_id'] = f'o-{random.randint(90000, 99999)}'
        log_entry['cart_value'] = round(random.uniform(50, 500), 2)

    return log_entry

def send_to_firehose(delivery_stream_name, batch_size=500):
    """Send data to Kinesis Firehose in batches with error handling"""
    records = []

    for _ in range(batch_size):
        log_entry = generate_log_entry()
        records.append({
            'Data': json.dumps(log_entry) + '\n'
        })

    try:
        response = firehose_client.put_record_batch(
            DeliveryStreamName=delivery_stream_name,
            Records=records
        )

        failed_count = response.get('FailedPutCount', 0)
        success_count = batch_size - failed_count

        if failed_count > 0:
            failed_records = [
                records[i] for i, record in enumerate(response.get('RequestResponses', []))
                if 'ErrorCode' in record
            ]
            print(f'Failed records: {len(failed_records)}, First error: {failed_records[0] if failed_records else "N/A"}')

        return success_count, failed_count

    except Exception as e:
        print(f'Error sending to Firehose: {str(e)}')
        return 0, batch_size

def main():
    delivery_stream_name = 'logprocessingstack-log-ingestion'
    total_records = 0
    total_failed = 0
    batch_size = 500

    try:
        while True:
            success, failed = send_to_firehose(delivery_stream_name, batch_size)
            total_records += success
            total_failed += failed

            print(f'Batch stats - Sent: {success}, Failed: {failed}')
            print(f'Total records - Sent: {total_records}, Failed: {total_failed}')

            time.sleep(0.01)

    except KeyboardInterrupt:
        print('\nStopping data streaming...')
        print(f'Final count - Successfully sent: {total_records}, Failed: {total_failed}')

if __name__ == '__main__':
    main()