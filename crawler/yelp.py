import boto3
import base64
import requests
import json


# Configuration
YELP_API_KEY: str
YELP_URL: str
CUISINES: list[str]
LOCATION: str
CUISINE_TOTAL: int
LIMIT: int
AWS_CREDENTIALS = {
    'aws_access_key_id': str,
    'aws_secret_access_key': str,
    'region_name': str
}
DYNAMO_TABLE_NAME: str
ES_MASTER_USER: str
ES_MASTER_PASSWORD: str
ES_DOMAIN_ENDPOINT: str
ES_INDEX: str
ES_TYPE: str
YELP_HEADERS = {
    'Authorization': 'Bearer %s' % YELP_API_KEY
}
ES_BASE_URL = '%s/%s/%s' % (ES_DOMAIN_ENDPOINT, ES_INDEX, ES_TYPE)
ES_AUTHORIZATION = base64.b64encode(('%s:%s' % (ES_MASTER_USER,ES_MASTER_PASSWORD)).encode('ascii')).decode('ascii')
ES_HEADERS = {
    'Authorization': 'Basic %s' % ES_AUTHORIZATION,
    'Content-Type': 'application/json'
}


dynamodb = boto3.resource('dynamodb', **AWS_CREDENTIALS)
table = dynamodb.Table(DYNAMO_TABLE_NAME)
with table.batch_writer() as batch:
    for cuisine in CUISINES:
        for offset in range(0, CUISINE_TOTAL, LIMIT):
            params = {
                'term': cuisine + ' restaurant',
                'location': LOCATION,
                'limit': LIMIT,
                'offset': offset
            }
            resp = requests.get(url=YELP_URL, headers=YELP_HEADERS, params=params)
            restaurants = resp.json().get('businesses')
            if not restaurants:
                continue
            for restaurant in restaurants:
                # Add full data to DynamoDB
                batch.put_item(
                    Item={
                        'business_id': restaurant['id'],
                        'name': restaurant['name'],
                        'address': restaurant['location']['display_address'],
                        'coordinates': {
                            'latitude': restaurant['coordinates']['latitude'],
                            'longitude': restaurant['coordinates']['longitude']
                        },
                        'number_of_reviews': restaurant['review_count'],
                        'rating': restaurant['rating'],
                        'zip_code': restaurant['location']['zip_code']
                    }
                )
                # Add (business_id, cuisine) to Elasticsearch
                payload = {
                    'business_id': restaurant['id'],
                    'cuisine': cuisine
                }
                requests.post(url='%s/%s' % (ES_BASE_URL, restaurant['id']), headers=ES_HEADERS, data=json.dumps(payload))
