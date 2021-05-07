import json
import csv

import boto3
import codecs
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def process_file(bucket, file, s3_client, queue, activity_queue):
    s3_bucket = s3_client.Bucket(bucket)
    s3_obj = s3_bucket.Object(file)
    uploaded_time = s3_obj.last_modified

    meta = compile_metadata({}, s3_obj.metadata)
    validate_requester(meta)
    realm = file.split('/')[2]
    file_name = file.split('/')[3]
    task_id = str(int(uploaded_time.timestamp() * 1000)) + '-' + file_name
    meta['realm'] = realm
    meta['taskId'] = task_id
    counter = 0
    if activity_queue:
        send_activity_message(meta, file_name, uploaded_time, counter, activity_queue)
        
    # This should stream file from s3 and process it line by line without uploading all file in memory
    for row in csv.DictReader(codecs.getreader("utf-8")(s3_obj.get()['Body'])):
        line = list(row.values())
        payload = meta.copy()
        payload['user'] = {
            'email': line[0],
            'name': line[1] if len(line) > 1 else None
        }
        # add empty header and properties because a presence of 'header' is used to determine a format of message
        queue.send_message(MessageBody=json.dumps({'payload': payload, 'headers': {}, 'properties': {}}))
        counter = counter + 1

    logger.info(f"Sent [{counter}] create user messages")
    s3_obj.delete()
    if activity_queue:
        send_activity_message(meta, file_name, uploaded_time, counter, activity_queue)
    return counter


def send_activity_message(meta, file_name, uploaded_time, expected_number, queue):
    payload = meta.copy()
    payload['taskType'] = 'CREATE_USER'
    payload['task'] = {
        'fileName': file_name,
        'uploadedTime': uploaded_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'expected': expected_number
    }
    queue.send_message(MessageBody=json.dumps({'payload': payload, 'headers': {}, 'properties': {}}))


def compile_metadata(r, s):
    for key in s:
        p = key.split('-')
        t = {}
        for i in p[:-1]:
            if not r.get(i):
                r[i] = t
            t = r[i]
        t[p[-1]] = s[key]
    return r


def validate_requester(meta):
    requester = meta.get('requester', {})
    expected_fields = ['exid', 'ip', 'city', 'countrycode']

    for f in expected_fields:
        if f not in requester:
            raise AttributeError('S3 objects must be configured with requester.exid, requester.ip, requester.city, '
                                 'requester.countrycode')


def lambda_handler(event, context):

    queue_name = os.environ['CREATE_USERS_SQS_QUEUE']
    s3_client = boto3.resource('s3')

    sqs_client = boto3.resource('sqs')
    queue = sqs_client.get_queue_by_name(QueueName=queue_name)
    send_bulk_activity_message = os.environ.get('SEND_BULK_ACTIVITY_MESSAGE', 'FALSE').upper() == 'TRUE'

    if send_bulk_activity_message:
        bulk_activity_queue_name = os.environ['BULK_ACTIVITY_SQS_QUEUE']
        activity_queue = sqs_client.get_queue_by_name(QueueName=bulk_activity_queue_name)
    else:
        activity_queue = False

    for rec in event.get('Records', []):
        bucket = rec.get('s3').get('bucket').get('name')
        file = rec.get('s3').get('object').get('key')
        logger.info(f"Process [{bucket}/{file}] started")
        try:
            processed = process_file(bucket, file, s3_client, queue, activity_queue)
            logger.info(f"Process [{bucket}/{file}] finished, processed [{processed}] records")
        except Exception as e:
            logger.error(e)
            raise e
