import time
import threading
import boto3


class KinesisConsumer(threading.Thread):
    """Producer class for AWS Kinesis streams"""

    def __init__(self, stream_name, sleep_interval=None):
        self.client = boto3.client('kinesis')
        self.stream_name = stream_name

        stream_details = self.client.describe_stream(
            StreamName=self.stream_name)
        shard_id = stream_details['StreamDescription']['Shards'][0]['ShardId']

        response = self.client.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'
        )
        self.currentShardIterator = response['ShardIterator']

        self.sleep_interval = sleep_interval
        super().__init__()

    def process_record(self, record):
        pass

    def get_record(self):
        response = self.client.get_records(
            ShardIterator=self.currentShardIterator, Limit=5)
        self.currentShardIterator = response['NextShardIterator']

        for record in response['Records']:
            if 'Data' in record and len(record['Data']) > 0:
                self.process_record(record['Data'])

    def run_continously(self):
        """put a record at regular intervals"""
        while True:
            try:
                self.get_record()
                time.sleep(self.sleep_interval)
            except Exception as e:
                print(e)
                break
