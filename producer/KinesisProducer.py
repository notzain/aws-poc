import time
import threading
import boto3


class KinesisProducer(threading.Thread):
    """Producer class for AWS Kinesis streams"""

    def __init__(self, stream_name, partitionKey, sleep_interval=None):
        self.client = boto3.client('kinesis')

        self.stream_name = stream_name
        self.partitionKey = partitionKey

        self.sleep_interval = sleep_interval
        super().__init__()

    def generate_data(self):
        pass

    def put_record(self):
        """put a single record to the stream"""
        response = self.client.put_record(
            StreamName=self.stream_name,
            Data=bytes(self.generate_data(), 'utf-8'),
            PartitionKey=self.partitionKey
        )
        print(response)

    def run_continously(self):
        """put a record at regular intervals"""
        while True:
            try:
                self.put_record()
                time.sleep(self.sleep_interval)
            except Exception as e:
                print(e)
                break
