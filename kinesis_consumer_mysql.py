import boto3
import json
import time
import mysql.connector

client = mysql.connector.connect(host='localhost', user='test', password='test', database='testdb', charset='utf8mb4', collation='utf8mb4_bin', autocommit=True)
client.ping(reconnect=True)
cursor = client.cursor(prepared=True)

stream_name = 'quickstart'
kinesis = boto3.client('kinesis', region_name='ap-northeast-1')
response = kinesis.describe_stream(StreamName=stream_name)
shard_id = response['StreamDescription']['Shards'][0]['ShardId']
shard_iterator = kinesis.get_shard_iterator(StreamName=stream_name,
                                            ShardId=shard_id,
                                            ShardIteratorType='LATEST')

last_operation = 'REFRESH'

def store_cdcdata(cdcdata):
  operation = cdcdata['message']['headers']['operation']
  employeeid = cdcdata['message']['data']['EmployeeID']
  lastname = cdcdata['message']['data']['LastName']
  firstname = cdcdata['message']['data']['FirstName']
  #コンソールに出力
  print(operation, employeeid, lastname, firstname, sep="\n")

  global client
  global last_operation
  #MySQLのテーブルをクリア
  if last_operation != 'REFRESH' and operation == 'REFRESH':
    print('Received RESET operation!!')
    cursor.execute('DELETE FROM fullload')
    cursor.execute('DELETE FROM changes')

  #MySQLにINSERT
  if operation == 'REFRESH':
    statement = 'INSERT INTO fullload(EmployeeID, LastName, FirstName) values(?, ?, ?)'
    try:
      cursor.execute(statement, (employeeid,lastname,firstname, ))
      #print(cursor.rowcount)
    except Exception as e:
      print(e)
  elif operation == 'INSERT' or operation == 'UPDATE' or operation == 'DELETE':
    statement = 'INSERT INTO changes(Operation, EmployeeID, LastName, FirstName) values(?, ?, ?, ?)'
    try:
      cursor.execute(statement, (operation,employeeid,lastname,firstname, ))
      #print(cursor.rowcount)
    except Exception as e:
      print(e)
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

cursor.close()
client.close()
