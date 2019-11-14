from KinesisConsumer import KinesisConsumer
import json
import argparse

parser = argparse.ArgumentParser(description='Consume data from Kinesis stream')
parser.add_argument('--stream', required=True, type=str)

args = parser.parse_args()

class DeviceConsumer(KinesisConsumer):
    def __init__(self, streamName, interval):
        super().__init__(streamName, interval)

    def process_record(self, record):
        print(json.loads(record))

consumer = DeviceConsumer(args.stream, 1)
consumer.run_continously()
