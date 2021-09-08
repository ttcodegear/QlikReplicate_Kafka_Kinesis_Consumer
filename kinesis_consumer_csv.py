import boto3
import json
import time
import csv

stream_name = 'quickstart'
kinesis = boto3.client('kinesis', region_name='ap-northeast-1')
response = kinesis.describe_stream(StreamName=stream_name)
shard_id = response['StreamDescription']['Shards'][0]['ShardId']
shard_iterator = kinesis.get_shard_iterator(StreamName=stream_name,
                                           ShardId=shard_id,
                                           ShardIteratorType='LATEST')

#エンコーディング "utf8"
fullload_f = open('C:\\temp\\fullload.csv', 'a', newline='', encoding='utf8')
fullload = csv.writer(fullload_f)
changes_f = open('C:\\temp\\changes.csv', 'a', newline='', encoding='utf8')
changes = csv.writer(changes_f)

last_operation = 'REFRESH'

def store_cdcdata(cdcdata):
  operation = cdcdata['message']['headers']['operation']
  employeeid = cdcdata['message']['data']['EmployeeID']
  lastname = cdcdata['message']['data']['LastName']
  firstname = cdcdata['message']['data']['FirstName']
  #コンソールに出力
  print(operation, employeeid, lastname, firstname, sep="\n")

  global last_operation
  #CSVファイルをクリア
  if last_operation != 'REFRESH' and operation == 'REFRESH':
    print('Received RESET operation!!')
    fullload_f.truncate(0)
    fullload.writerow(['EmployeeID','LastName','FirstName'])
    fullload_f.flush()
    changes_f.truncate(0)
    changes.writerow(['Operation','EmployeeID','LastName','FirstName'])
    changes_f.flush()

  #CSVファイルに追記
  if operation == 'REFRESH':
    fullload.writerow([employeeid,lastname,firstname])
    fullload_f.flush()
  elif operation == 'INSERT' or operation == 'UPDATE' or operation == 'DELETE':
    changes.writerow([operation,employeeid,lastname,firstname])
    changes_f.flush()
  last_operation = operation

try:
  records = kinesis.get_records(ShardIterator=shard_iterator['ShardIterator'], Limit=1)
  if len(records['Records']) > 0:
    #コンソールに出力
    #print(records['Records'])
    cdcdata = json.loads(records['Records'][0]['Data'].decode())
    store_cdcdata(cdcdata)
  time.sleep(0.1)
  while 'NextShardIterator' in records:
    records = kinesis.get_records(ShardIterator=records['NextShardIterator'], Limit=1)
    if len(records['Records']) > 0:
      #コンソールに出力
      #print(records['Records'])
      cdcdata = json.loads(records['Records'][0]['Data'].decode())
      store_cdcdata(cdcdata)
    time.sleep(0.1)
except Exception as e:
  print(e)
except KeyboardInterrupt:
  pass

fullload_f.close()
changes_f.close()
