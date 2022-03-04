import logging
import boto3
import math
import dateutil.parser
import datetime
import time
import os


# Configuration
SQS_URL: str
CUISINE_TYPES: list[str]
LOCATION: str

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


""" --- Helpers for Lex response --- """


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


def plain_text(content):
    return {
        'contentType': 'PlainText',
        'content': content
    }


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            'isValid': is_valid,
            'violatedSlot': violated_slot,
        }
    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': plain_text(message_content)
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def validate_dining_suggestions(slots):
    if slots['cuisine'] is not None and slots['cuisine'].lower() not in CUISINE_TYPES:
        return build_validation_result(False,
                                       'cuisine',
                                       'We do not have suggestions for %s, would you like a different cuisine?  '
                                       'Our most popular cuisine is %s' % (slots['cuisine'], CUISINE_TYPES[0]))
    if slots['location'] is not None and slots['location'].lower() != LOCATION:
        return build_validation_result(False,
                                       'location',
                                       'We do not have suggestions in %s, would you like a different location?  '
                                       'Our most popular location is %s' % (slots['location'], LOCATION))
    if slots['dining_date'] is not None:
        if not isvalid_date(slots['dining_date']):
            return build_validation_result(False,
                                           'dining_date',
                                           'I did not understand that, what date would you like for dining suggestions?')
        elif datetime.datetime.strptime(slots['dining_date'], '%Y-%m-%d').date() <= datetime.date.today():
            return build_validation_result(False,
                                           'dining_date',
                                           'Please choose a time from tomorrow onwards, what date would you like for dining suggestions?')
    if slots['dining_time'] is not None:
        if len(slots['dining_time']) != 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'dining_time', None)
        hour, minute = slots['dining_time'].split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'dining_time', None)
        if hour < 9 or hour > 18:
            # Outside of business hours
            return build_validation_result(False,
                                           'dining_time',
                                           'Our business hours are from nine a m. to six p m. Can you specify a time during this range?')
    return build_validation_result(True, None, None)


def sqs_push(slots):
    sqs = boto3.client('sqs')
    resp = sqs.send_message(
        QueueUrl=SQS_URL,
        MessageAttributes={
            'location': {
                'DataType': 'String',
                'StringValue': slots['location']
            },
            'cuisine': {
                'DataType': 'String',
                'StringValue': slots['cuisine']
            },
            'people_number': {
                'DataType': 'String',
                'StringValue': slots['people_number']
            },
            'dining_date': {
                'DataType': 'String',
                'StringValue': slots['dining_date']
            },
            'dining_time': {
                'DataType': 'String',
                'StringValue': slots['dining_time']
            },
            'phone_number': {
                'DataType': 'String',
                'StringValue': slots['phone_number']
            }
        },
        MessageBody='Dining Requirements'
    )
    logger.debug(resp)


""" --- Handlers for Lex intent --- """


def greeting(intent_request):
    return close(intent_request['sessionAttributes'], 'Fulfilled', plain_text('Hi there, how can I help?'))


def thank_you(intent_request):
    return close(intent_request['sessionAttributes'], 'Fulfilled', plain_text('You’re welcome.'))


def dining_suggestions(intent_request):
    # requirement = dining_requirement(intent_request)
    slots = get_slots(intent_request)
    source = intent_request['invocationSource']
    if source == 'DialogCodeHook':
        # Perform basic validation on the supplied input slots.
        # Use the elicitSlot dialog action to re-prompt for the first violation detected.
        validation_result = validate_dining_suggestions(slots)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])
        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return delegate(output_session_attributes, get_slots(intent_request))
    sqs_push(slots)
    return close(intent_request['sessionAttributes'], 'Fulfilled', plain_text('You’re all set. Expect my suggestions shortly! Have a good day.'))


def dispatch(intent_request):
    logger.debug('dispatch userId=%s, intentName=%s' % (intent_request['userId'], intent_request['currentIntent']['name']))
    intent_name = intent_request['currentIntent']['name']
    # Dispatch to intent handlers
    if intent_name == 'greeting':
        return greeting(intent_request)
    if intent_name == 'thank-you':
        return thank_you(intent_request)
    if intent_name == 'dining-suggestions':
        return dining_suggestions(intent_request)
    raise Exception('Intent with name %s not supported' % intent_name)


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name=%s' % event['bot']['name'])
    return dispatch(event)
