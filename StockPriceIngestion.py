import datetime
import json

import boto3
import yfinance as yf

# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc.
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream
kinesis_client = boto3.client('kinesis',
                              region_name="us-east-1")  # Modify this line of code according to your requirement.


# function to push data to kinesis stream
def push_payload_to_stream(payload, stream_name="stock_price_stream"):
    # payload = {'stockid': 'MSFT', 'price': 273.1700134277344, 'timestamp': '2022-06-06T09:30:00.000000-0400', '52WeekHigh': 349.67, '52WeekLow': 246.44}
    # print("Payload : ",payload)
    print("Sending ", payload, " to stream ", stream_name)
    put_response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(payload),
        PartitionKey='stockid')


def read_from_stream(stream_name='stock_price_stream'):

    print("\n\n Reading data from stream : ",stream_name)
    response = kinesis_client.describe_stream(StreamName=stream_name)
    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name,
                                                       ShardId=my_shard_id,
                                                       ShardIteratorType='TRIM_HORIZON')

    my_shard_iterator = shard_iterator['ShardIterator']

    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,Limit=20)
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],Limit=20)
    print(record_response)


today = datetime.date.today()
dbf_yesterday = datetime.date.today() - datetime.timedelta(2)

stocks_list = ['MSFT', 'MVIS', 'GOOG', 'SPOT', 'INO', 'OCGN', 'ABML', 'RLLCF', 'JNJ', 'PSFE']

data_set = {}
print('fetching data for stock set', stocks_list)
stock_data = []
for stock in stocks_list:
    # Example of pulling the data between 2 dates from yfinance API
    print('\n fetching {0}'.format(stock))

    ## Add code to pull the data for the stocks specified in the doc
    data_dump = yf.download(stock, start=dbf_yesterday, end=today, interval='1h')

    ## Add additional code to call 'info' API to get 52WeekHigh and 52WeekLow refering this this link - https://pypi.org/project/yfinance/
    print('\t fetching ticker for {0}'.format(stock))
    ticker = yf.Ticker(stock)

    for index, data in data_dump.iterrows():
        # print(data)
        # print(index)
        stock_data.append({
            'stockid': stock,
            'price': data['Close'],
            'timestamp': index.strftime('%Y-%m-%d'),
            '52WeekHigh': ticker.info['fiftyTwoWeekHigh'],
            '52WeekLow': ticker.info['fiftyTwoWeekLow'],
        })
# display first 10 records from list
count = 10 if len(stock_data) > 10 else len(stock_data) - 1
print('Data read successfully. \n \t data count  :', len(stock_data), "\n \tfirst ", count, " rows ", stock_data[:count],
      '\n\n ==> Streaming data to kinesis stream now \n')

# Add your code here to push data records to Kinesis stream.
[push_payload_to_stream(data) for data in stock_data]

# Read data from kinesis stream
read_from_stream()
