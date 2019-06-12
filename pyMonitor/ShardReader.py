#!/usr/bin/env python3

import json, time, boto3
from threading import Thread
from botocore.exceptions import ClientError

class ShardReader(Thread):
  def __init__(self, name, stream_name, shard_id):
    super(ShardReader, self).__init__(None, name)
    self.name = name
    self.stream_name = stream_name
    self.shard_id = shard_id

  @staticmethod
  def detect_incident(event):
    decoded = json.loads(event)
    passed = None, None
    try:
      server = decoded['on']['server']['hostname']
      metrics = decoded['direct_object']['filesystem_metrics']
      pct_avail = metrics['available'] * 100 / metrics['size']
      return (server, pct_avail) if pct_avail <= 20 else passed
    except KeyError:
      return passed

  def run(self):
    try:
      next_iterator = conn.get_shard_iterator(StreamName=self.stream_name, 
        ShardId=self.shard_id, ShardIteratorType='TRIM_HORIZON')['ShardIterator']
      while True:
        response = conn.get_records(ShardIterator=next_iterator, Limit=10)
        for event in response['Records']:
          print(f"{self.name} read event {event['PartitionKey']}")
          s, a = self.detect_incident(event['Data'])
          if a:
            print(f'{s} has only {a}% disk available!')
        next_iterator = response['NextShardIterator']
        time.sleep(5)
    except ProvisionedThroughputExceededException as ptee:
      print(f'Caught: {ptee.message}')
      time.sleep(5)

if __name__ == '__main__':
  session = boto3.Session(profile_name="personal")
  conn = session.client("kinesis", region_name="us-east-1")
  stream = conn.describe_stream(StreamName="events")
  shards = stream['StreamDescription']['Shards']
  
  threads = []
  for shard in shards:
    shard_id = shard['ShardId']
    reader_name = f'Reader-{shard_id}'
    reader = ShardReader(reader_name, 'events', shard_id)
    reader.start()
    threads.append(reader)

  for thread in threads:
    thread.join()