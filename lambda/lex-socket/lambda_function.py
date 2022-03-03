import boto3


# Configuration
BOT_NAME: str
BOT_ALIAS: str
USER_ID: str


def lambda_handler(event, context):
    lex = boto3.client('lex-runtime')
    resp = lex.post_text(botName=BOT_NAME, botAlias=BOT_ALIAS, userId=USER_ID, inputText=event['messages'][0]['unstructured']['text'])
    return {
        'messages': [
            {
                'type': 'unstructured',
                'unstructured': {
                    'text': resp['message']
                }
            }
        ]
    }
