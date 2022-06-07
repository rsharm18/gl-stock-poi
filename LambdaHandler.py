import base64
import json
from decimal import Decimal

import boto3

print('Loading function')


def isPOI(payload):
    print("Payload ", payload)
    fifty_two_week_high = payload['52WeekHigh']
    fifty_two_week_low = payload['52WeekLow']
    price = payload['price']
    if price >= 0.8 * fifty_two_week_high or price <= 1.2 * fifty_two_week_low:
        return True
    return False


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    processed_stock = {}
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')

        payload_json = json.loads(payload)
        print(processed_stock)
        if isPOI(payload_json) and payload_json['stockid'] not in processed_stock.keys() :
            processed_stock[payload_json['stockid']] = payload_json['stockid']
            print("POI payload: ", payload_json)
            handle_poi_data(payload_json)
        else:
            print("SKIP ", payload_json)

    return 'Successfully processed {} records.'.format(len(event['Records']))

def handle_poi_data(payload):
    send_sns_notification(payload)
    insert_POI_to_dynamodb(payload)

def send_sns_notification(payload,topic_arn='arn:aws:sns:us-east-1:185138245721:stock-poi'):
    sns_client = boto3.client('sns')
    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject='Stock - POI {0}'.format(payload['stockid']),
        Message='POI stock received'
    )
    print("SNS alert sent : ", response)

def insert_POI_to_dynamodb(payload,table_name='stock_poi'):
    print(payload)
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.put_item(
        Item=json.loads(json.dumps(payload), parse_float=Decimal)
    )
    print("Dynamodb record added" , response)
