import boto3
import base64
import urllib3
import json
from boto3.dynamodb.conditions import Attr


# Configuration
SQS_URL: str
DYNAMO_TABLE_NAME: str
ES_MASTER_USER: str
ES_MASTER_PASSWORD: str
ES_DOMAIN_ENDPOINT: str
ES_INDEX: str
ES_TYPE: str
ES_RESULT_SIZE: int
ES_BASE_URL = '%s/%s/%s' % (ES_DOMAIN_ENDPOINT, ES_INDEX, ES_TYPE)
ES_AUTHORIZATION = base64.b64encode(('%s:%s' % (ES_MASTER_USER,ES_MASTER_PASSWORD)).encode('ascii')).decode('ascii')
ES_HEADERS = {
    'Authorization': 'Basic %s' % ES_AUTHORIZATION,
    'Content-Type': 'application/json'
}


def sqs_pop():
    sqs = boto3.client('sqs')
    resp = sqs.receive_message(
        QueueUrl=SQS_URL,
        AttributeNames=['SentTimestamp'],
        MessageAttributeNames=['All']
    )
    message = resp['Messages'][0]
    sqs.delete_message(
        QueueUrl=SQS_URL,
        ReceiptHandle=message['ReceiptHandle']
    )
    return message


def get_random_ids_by_cuisine(cuisine):
    data = {
        'query': {
            'function_score': {
                'query': {
                    'match': {
                        'cuisine': cuisine
                    }
                },
                'random_score': {}
            }
        },
        'size': ES_RESULT_SIZE
    }
    http = urllib3.PoolManager()
    resp = http.request('GET', ES_BASE_URL + '/_search', headers=ES_HEADERS, body=json.dumps(data))
    resp = json.loads(resp.data)['hits']['hits']
    return [resp[i]['_id'] for i in range(ES_RESULT_SIZE)]


def get_restaurants_by_ids(business_ids):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_TABLE_NAME)
    return [table.scan(FilterExpression=Attr('business_id').eq(business_id))['Items'][0] for business_id in business_ids]


def sns_send(message, phone_number):
    sns = boto3.client('sns')
    sns.publish(
        PhoneNumber=phone_number,
        Message=message
    )


def lambda_handler(event, context):
    sqs_message = sqs_pop()['MessageAttributes']
    location = sqs_message['Location']['StringValue']
    cuisine = sqs_message['Cuisine']['StringValue']
    people_number = sqs_message['PeopleNumber']['StringValue']
    dining_date = sqs_message['DiningDate']['StringValue']
    dining_time = sqs_message['DiningTime']['StringValue']
    phone_number = sqs_message['PhoneNumber']['StringValue']
    business_ids = get_random_ids_by_cuisine(cuisine)
    restaurants = get_restaurants_by_ids(business_ids)
    sns_message = 'Hello! Here are my %s restaurant suggestions in %s for %s people, for %s at %s: ' % (cuisine, location, people_number, dining_date, dining_time)
    for idx, restaurant in enumerate(restaurants):
        sns_message += '%d. %s, located at %s. ' % (idx + 1, restaurant['name'], ' '.join(restaurant['address']))
    sns_message += 'Enjoy your meal!'
    sns_send(sns_message, phone_number)