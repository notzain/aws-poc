from KinesisProducer import KinesisProducer
import json
import random
import argparse

parser = argparse.ArgumentParser(description='Generate random device data')
parser.add_argument('--stream', required=True, type=str)
parser.add_argument('--devices', required=True, type=int)
parser.add_argument('--min', type=int, default=80)
parser.add_argument('--max', type=int, default=100)

args = parser.parse_args()


class Device:
    def __init__(self, uuid, health):
        self.uuid = uuid
        self.health = health

    def serialize(self):
        return {
            "uuid": self.uuid,
            "health": self.health
        }


class Link:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def serialize(self):
        return {
            "from": self.a,
            "to": self.b
        }


class DeviceList:
    def __init__(self):
        self.devices = []
        self.links = []

    def addDevice(self, device):
        self.devices.append(device)

    def addLink(self, link):
        self.links.append(link)

    def updateDevice(self, uuid, health):
        for device in self.devices:
            if device.uuid == uuid:
                device.health = health
                break


class DeviceProducer(KinesisProducer):
    def __init__(self, streamName, partitionKey, interval=1):
        self.deviceList = DeviceList()
        super().__init__(streamName, partitionKey, interval)

    def startup(self):
        def create_message(device):
            return {
                "type": "Add",
                **device.serialize()
            }

        messages = json.dumps(list(map(lambda device: create_message(
            device), self.deviceList.devices)))

        response = self.client.put_records(
            Records=[
                {
                    'Data': bytes(messages, 'utf-8'),
                    'PartitionKey': self.partitionKey
                },
            ],
            StreamName=self.stream_name
        )
        print(response)

    def generate_data(self):
        return json.dumps({
            "type": "Update",
            "uuid": random.randint(1, len(self.deviceList.devices)),
            "health": random.randint(args.min, args.max)
        })


producer = DeviceProducer(args.stream, "PROCENTEC", 1)
for i in range(args.devices):
    producer.deviceList.addDevice(Device(uuid=i + 1, health=100))

producer.run_continously()
