import base64
import json
from decimal import Decimal

import boto3


def is_poi(payload):
    print("Payload ", payload)
    fifty_two_week_high = payload['52WeekHigh']
    fifty_two_week_low = payload['52WeekLow']
    price = payload['price']
    # A particular price is a POI (point of interest) if it’s either >= 80% of 52WeekHigh or <= 120% of 52WeekLow
    return price >= 0.8 * fifty_two_week_high or price <= 1.2 * fifty_two_week_low


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')

        payload_json = json.loads(payload)
        if is_poi(payload_json):
            print("POI payload: ", payload_json)
            handle_poi_data(payload_json)
        else:
            print("SKIP ", payload_json)

    return 'Successfully processed {} records.'.format(len(event['Records']))


def handle_poi_data(payload):
    if not is_alert_raised(payload['stockid'], payload['timestamp']):
        send_sns_notification(payload)
        insert_poi_to_dynamodb(payload)
    else:
        print("Skip POI ", payload, " as the alert is already raised")


def send_sns_notification(payload, topic_arn='arn:aws:sns:us-east-1:185138245721:stock-poi'):
    sns_client = boto3.client('sns')
    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject='Stock - POI {0}'.format(payload['stockid']),
        Message=json.dumps(payload)
    )
    print("SNS alert sent : ", response)


def insert_poi_to_dynamodb(payload, table_name='stock_poi'):
    print(payload)
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.put_item(
        Item=json.loads(json.dumps(payload), parse_float=Decimal)
    )
    print("Dynamodb record added", response)


def is_alert_raised(stockid, timestamp, table_name='stock_poi'):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.get_item(
        Key={
            'stockid': stockid,
            'timestamp': timestamp
        }
    )
    # check if the property item exists in response. presence of item indicates record exist in dynamodb for the given param
    return 'Item' in response.keys()
