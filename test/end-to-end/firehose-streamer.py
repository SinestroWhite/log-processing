import json
import boto3
import time
import random
from datetime import datetime, timezone
import multiprocessing as mp
from typing import Dict, List, Tuple

from botocore.config import Config

my_config = Config(
    region_name='eu-central-1'
)

# Initialize base structures
SERVICES = {
    'checkout': ['checkout_started', 'checkout_completed', 'checkout_failed'],
    'order': ['order_created', 'order_failed', 'order_cancelled'],
    'search': ['search_results', 'autosuggest', 'search_failed'],
    'cart': ['add_to_cart', 'remove_from_cart', 'cart_expired'],
    'payment': ['payment_authorized', 'payment_failed', 'payment_retry'],
    'auth': ['login_success', 'login_fail', 'password_reset_requested']
}

ERROR_SCENARIOS = {
    'payment': {'insufficient_funds': 'Payment declined', 'rate_limit': 'Rate limited'},
    'auth': {'invalid_credentials': 'Invalid login', 'account_locked': 'Account locked'},
    'catalog': {'database_error': 'DB timeout', 'invalid_sku': 'SKU not found'}
}

def generate_log_entry() -> Dict:
    """Generate a single log entry"""
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

    if level == 'ERROR' and service in ERROR_SCENARIOS:
        error_type = random.choice(list(ERROR_SCENARIOS[service].keys()))
        log_entry['error'] = {
            'type': error_type,
            'message': ERROR_SCENARIOS[service][error_type],
            'error_code': f'E{random.randint(1000, 9999)}'
        }

    return log_entry

def worker_send_to_firehose(args: Tuple[str, int, int]) -> Tuple[int, int]:
    """Worker function for parallel processing"""
    delivery_stream_name, batch_size, worker_id = args
    firehose_client = boto3.client('firehose', config=my_config)
    success_count = 0
    failed_count = 0

    try:
        records = [
            {'Data': json.dumps(generate_log_entry()) + '\n'}
            for _ in range(batch_size)
        ]

        response = firehose_client.put_record_batch(
            DeliveryStreamName=delivery_stream_name,
            Records=records
        )

        failed_count = response.get('FailedPutCount', 0)
        success_count = batch_size - failed_count

        print(f'Worker {worker_id}: Sent {success_count} records, Failed {failed_count}')

    except Exception as e:
        print(f'Worker {worker_id} error: {str(e)}')
        failed_count = batch_size
        success_count = 0

    return success_count, failed_count

def parallel_send_to_firehose(delivery_stream_name: str,
                              total_records: int = 10000,
                              batch_size: int = 500,
                              num_processes: int = 4) -> Tuple[int, int]:
    """Send data to Firehose using multiple processes"""

    num_batches = total_records // batch_size
    if total_records % batch_size:
        num_batches += 1

    # Create pool of workers
    pool = mp.Pool(processes=num_processes)

    # Prepare arguments for each worker
    worker_args = [
        (delivery_stream_name, batch_size, i)
        for i in range(num_batches)
    ]

    # Execute tasks in parallel
    results = pool.map(worker_send_to_firehose, worker_args)

    # Clean up
    pool.close()
    pool.join()

    # Aggregate results
    total_success = sum(r[0] for r in results)
    total_failed = sum(r[1] for r in results)

    return total_success, total_failed

def main():
    delivery_stream_name = 'logprocessingstack-log-ingestion'
    total_records = 100_000_000  # Total records to send
    batch_size = 500       # Records per batch
    num_processes = 64      # Number of parallel processes

    print(f'Starting parallel ingestion with {num_processes} processes')
    print(f'Total records: {total_records}, Batch size: {batch_size}')

    start_time = time.time()

    try:
        success, failed = parallel_send_to_firehose(
            delivery_stream_name=delivery_stream_name,
            total_records=total_records,
            batch_size=batch_size,
            num_processes=num_processes
        )

        duration = time.time() - start_time
        records_per_second = total_records / duration

        print('\nIngestion completed:')
        print(f'Successfully sent: {success} records')
        print(f'Failed: {failed} records')
        print(f'Duration: {duration:.2f} seconds')
        print(f'Throughput: {records_per_second:.2f} records/second')

    except KeyboardInterrupt:
        print('\nStopping data streaming...')

if __name__ == '__main__':
    # Ensure proper multiprocessing behavior on different platforms
    mp.freeze_support()
    main()