import json
import boto3
import sys
import yfinance as yf

import time
import random
import datetime

def isPOI(payload):
    fifty_two_week_high = payload['52WeekHigh']
    fifty_two_week_low = payload['52WeekLow']
    price = payload['price']
    if price >= 0.8 * fifty_two_week_high or price <= 1.2 * fifty_two_week_low:
        return True
    return False

# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc.
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream
kinesis_client = boto3.client('kinesis',
                           region_name="us-east-1")  # Modify this line of code according to your requirement.


load = {
    "stockid": "GOOG",
    "price": 2353.8701171875,
    "timestamp": "2022-06-06T10:30:00.000000-0400",
    "52WeekHigh": 3042,
    "52WeekLow": 2044.16
}
print(isPOI(load))
def put_to_stream(payload,stream_name="stock_price_stream"):

    # payload = {'stockid': 'MSFT', 'price': 273.1700134277344, 'timestamp': '2022-06-06T09:30:00.000000-0400', '52WeekHigh': 349.67, '52WeekLow': 246.44}
    print("Payload : ",payload)
    print(" stream_name ",stream_name)
    put_response = kinesis_client.put_record(
                        StreamName=stream_name,
                        Data=json.dumps(payload),
                        PartitionKey='stockid')

def read_from_stream(stream_name='stock_price_stream'):
    response = kinesis_client.describe_stream(StreamName=stream_name)
    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name,
                                                       ShardId=my_shard_id,
                                                       ShardIteratorType='TRIM_HORIZON')

    my_shard_iterator = shard_iterator['ShardIterator']

    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,
                                                 Limit=2)

    while 'NextShardIterator' in record_response:
        record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],
                                                     Limit=2)
        print(record_response)
        # wait for 5 seconds
        time.sleep(5)

today = datetime.date.today()
yesterday = datetime.date.today() - datetime.timedelta(1)

# put_to_stream({})
stocks_list = ['MSFT', 'MVIS', 'GOOG', 'SPOT', 'INO', 'OCGN', 'ABML', 'RLLCF', 'JNJ', 'PSFE']
# stocks_list = ['MSFT', 'MVIS', 'GOOG']

data_set = {}

for stock in stocks_list:
    # Example of pulling the data between 2 dates from yfinance API
    print('fetching {0}'.format(stock))
    stock_data = []
    ## Add code to pull the data for the stocks specified in the doc
    data_dump = yf.download(stock, start=yesterday, end=today, interval='1h')

    ## Add additional code to call 'info' API to get 52WeekHigh and 52WeekLow refering this this link - https://pypi.org/project/yfinance/
    ticker = yf.Ticker(stock)

    for index, data in data_dump.iterrows():
        # print(data)
        # print(index)
        stock_data.append({
            'stockid': stock,
            'price': data['Close'],
            'timestamp': index.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
            '52WeekHigh': ticker.info['fiftyTwoWeekHigh'],
            '52WeekLow': ticker.info['fiftyTwoWeekLow'],
        })
        [put_to_stream(data) for data in stock_data]
    # data_set[stock] = stock_data

# print(stock_data)
# read_from_stream()

## Add your code here to push data records to Kinesis stream.
