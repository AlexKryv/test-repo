import boto3
import pytest
from freezegun import freeze_time

from moto import mock_s3, mock_sqs
from src import lambda_function

_region = 'eu-west-1'
_bucket = 'test-dce-id-users-bulk-operation'
_event = {
    'Records': [
        {
            's3': {
                'bucket': {'name': 'test-dce-id-users-bulk-operation'},
                'object': {'key': 'test/create-users/dce.test/users.csv'}
            }
        }
    ]
}


def aws_credentials(monkeypatch):
    """Mocked AWS Credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", _region)

    """Mocked lambda environment variables"""
    monkeypatch.setenv('CREATE_USERS_SQS_QUEUE', 'CREATE_USERS_SQS_QUEUE')
    monkeypatch.setenv('BULK_ACTIVITY_SQS_QUEUE', 'BULK_ACTIVITY_SQS_QUEUE')
    monkeypatch.setenv('SEND_BULK_ACTIVITY_MESSAGE', 'TRUE')


def prepare_env(monkeypatch):
    aws_credentials(monkeypatch)
    s3 = boto3.client('s3', region_name=_region)
    sqs = boto3.client('sqs', region_name=_region)
    s3.create_bucket(Bucket=_bucket, CreateBucketConfiguration={'LocationConstraint': _region})

    sqs.create_queue(QueueName='CREATE_USERS_SQS_QUEUE')
    sqs.create_queue(QueueName='BULK_ACTIVITY_SQS_QUEUE')


@freeze_time("2012-01-01 12:00:00")
@mock_s3
@mock_sqs
def test_should_fails_if_no_requester_meta(monkeypatch):
    prepare_env(monkeypatch)
    s3 = boto3.client('s3', region_name=_region)
    s3_key = 'test/create-users/dce.test/users.csv'
    meta = {}
    s3.put_object(Bucket=_bucket, Key=s3_key, Body='email, name\nuser1@mail.com,User1\nuser2@mail.com,User2',
                  Metadata=meta)
    # When
    with pytest.raises(AttributeError):
        lambda_function.lambda_handler(_event, {})


@freeze_time("2012-01-01 12:00:00")
@mock_s3
@mock_sqs
def test_should_not_send_activity_message(monkeypatch):
    prepare_env(monkeypatch)
    monkeypatch.setenv('SEND_BULK_ACTIVITY_MESSAGE', 'FALSE')

    s3 = boto3.client('s3', region_name=_region)
    s3_key = 'test/create-users/dce.test/users.csv'
    meta = {
        'requester-exid': 'admin|exid',
        'requester-ip': '1.2.3.4',
        'requester-city': 'Minas Tirith',
        'requester-countrycode': 'Gondor'
    }
    s3.put_object(Bucket=_bucket, Key=s3_key, Body='email, name\nuser1@mail.com,User1\nuser2@mail.com,User2',
                  Metadata=meta)

    # When
    lambda_function.lambda_handler(_event, {})

    # What
    sqs = boto3.client('sqs', region_name=_region)
    mes1 = sqs.receive_message(QueueUrl='CREATE_USERS_SQS_QUEUE', MaxNumberOfMessages=2)
    assert len(mes1.get('Messages')) == 2

    mes2 = sqs.receive_message(QueueUrl='BULK_ACTIVITY_SQS_QUEUE', MaxNumberOfMessages=2)
    assert 'Messages' not in mes2


@freeze_time("2012-01-01 12:00:00")
@mock_s3
@mock_sqs
def test_should_get_file_from_s3(monkeypatch):
    prepare_env(monkeypatch)
    s3 = boto3.client('s3', region_name=_region)
    s3_key = 'test/create-users/dce.test/users.csv'
    meta = {
        'requester-exid': 'admin|exid',
        'requester-ip': '1.2.3.4',
        'requester-city': 'Minas Tirith',
        'requester-countrycode': 'Gondor'
    }
    s3.put_object(Bucket=_bucket, Key=s3_key, Body='email, name\nuser1@mail.com,User1\nuser2@mail.com,User2',
                  Metadata=meta)

    # When
    lambda_function.lambda_handler(_event, {})

    # What
    sqs = boto3.client('sqs', region_name=_region)
    mes1 = sqs.receive_message(QueueUrl='CREATE_USERS_SQS_QUEUE', MaxNumberOfMessages=2)
    payload = mes1.get('Messages')[0].get('Body')
    assert payload == '{"payload": {"requester": {"exid": "admin|exid", "ip": "1.2.3.4", "city": "Minas Tirith", ' \
                      '"countrycode": "Gondor"}, ' \
                      '"realm": "dce.test", "taskId": "1325419200000-users.csv", "user": {"email": ' \
                      '"user1@mail.com", "name": "User1"}}, "headers": {}, "properties": {}}'
    payload = mes1.get('Messages')[1].get('Body')
    assert payload == '{"payload": {"requester": {"exid": "admin|exid", "ip": "1.2.3.4", "city": "Minas Tirith", ' \
                      '"countrycode": "Gondor"}, ' \
                      '"realm": "dce.test", "taskId": "1325419200000-users.csv", "user": {"email": ' \
                      '"user2@mail.com", "name": "User2"}}, "headers": {}, "properties": {}}'

    mes1 = sqs.receive_message(QueueUrl='BULK_ACTIVITY_SQS_QUEUE', MaxNumberOfMessages=2)
    payload = mes1.get('Messages')[0].get('Body')
    assert payload == '{"payload": {"requester": {"exid": "admin|exid", "ip": "1.2.3.4", "city": "Minas Tirith", ' \
                      '"countrycode": "Gondor"}, ' \
                      '"realm": "dce.test", "taskId": "1325419200000-users.csv", "taskType": ' \
                      '"CREATE_USER", "task": {"fileName": "users.csv", "uploadedTime": "2012-01-01T12:00:00.000000Z", ' \
                      '"expected": 0}}, "headers": {}, "properties": {}}'

    payload = mes1.get('Messages')[1].get('Body')
    assert payload == '{"payload": {"requester": {"exid": "admin|exid", "ip": "1.2.3.4", "city": "Minas Tirith", ' \
                      '"countrycode": "Gondor"}, ' \
                      '"realm": "dce.test", "taskId": "1325419200000-users.csv", "taskType": ' \
                      '"CREATE_USER", "task": {"fileName": "users.csv", "uploadedTime": "2012-01-01T12:00:00.000000Z", ' \
                      '"expected": 2}}, "headers": {}, "properties": {}}'

    # File been deleted from s3
    s3_obj = s3.list_objects(Bucket=_bucket)
    assert 'Contents' not in s3_obj
