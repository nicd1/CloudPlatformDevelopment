import boto3
from botocore.exceptions import ClientError
import json
import glob
import time

sqs = boto3.resource('sqs')
sqsClient = boto3.client('sqs')
client = boto3.client("cloudformation")
s3client = boto3.client('s3')
s3 = boto3.resource('s3')
lambdaClient = boto3.client('lambda')

#Taken from response by Jusin Barton in https://stackoverflow.com/questions/23019166/boto-what-is-the-best-way-to-check-if-a-cloudformation-stack-is-exists
#Checks to see if stack already exists and has not failed
def stackStatus(name, required_status = 'CREATE_COMPLETE'):
    try:
        data = client.describe_stacks(StackName = name)
    except ClientError:
        return False
    return data['Stacks'][0]['StackStatus'] == required_status

name = 'stackS1915348'

if stackStatus(name):
    print("Stack already exists")
else:
#Creates stack as shown in https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudformation.html?highlight=create%20stack#CloudFormation.Client.create_stack
    response = client.create_stack(
        StackName=name,
        TemplateURL='https://s3.eu-west-2.amazonaws.com/cloudformation-templates-eu-west-2/DynamoDB_Table.template',
        Parameters=[
        {
            'ParameterKey': 'HashKeyElementName',
            'ParameterValue': 'hashkeyS1915348',
        },
    ],
    )
queue_name='sqss1915348'

#Creating a queue as shown in https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html#using-an-existing-queue
try:
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    print("Queue already exists")
except ClientError as error:
#If queue does not already exist, will create new queue
    if error.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
        queue = sqs.create_queue(QueueName=queue_name, Attributes={'DelaySeconds': '30'})

bucket_name = 's3buckets1915348'
region = 'eu-west-2'

#determines status of bucket using head_bucket() as shown in https://stackoverflow.com/questions/31092056/how-to-create-a-s3-bucket-using-boto3
#uses https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_bucket from boto3 documentation
def bucketStatus(bucket_name):
    try:
        response = s3client.head_bucket(Bucket=bucket_name)
        print('Bucket already exists')
        return True
    except ClientError as error:
        errorCode = int(error.response['Error']['Code'])
        if errorCode == 403:
            print ('Bucket is private, access is forbidden to this account')
            return True
        elif errorCode == 404:
#if bucket does not exist, head_bucket() return 404 error
            return False

#if bucketStatus() returns False, bucket of specified name does not exist
#if bucketStatus() is not True, new bucket will be created
#https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html
if bucketStatus(bucket_name) is not True:
    try:
        location = {'LocationConstraint': region}
        bucket = s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)   
    except ClientError as error:
        print(error)

queue_arn = queue.attributes['QueueArn']

def lambdaTrigger(queue_arn):
    try:
        response = lambdaClient.create_event_source_mapping(
        EventSourceArn=queue_arn,
        FunctionName='transcribeS1915348',
        BatchSize=5
    )
    except ClientError as error:
        if  error.response['Error']['Code'] != 'ResourceConflictException':
            raise

lambdaTrigger(queue_arn)

#https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
def sendFile(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name
    try:
        response = s3client.upload_file(file_name, bucket, object_name)
        time.sleep(30)
    except ClientError as error:
        print(error)
        return False
    return True

paths = glob.glob('audio/*.mp3', recursive=False)

for path in paths:
    obj_name = path.split("\\")[1]
    sendFile(path, bucket_name, obj_name)
    url = f"s3://{bucket_name}/{obj_name}"
    response = sqsClient.send_message(
        QueueUrl=queue.url,
        MessageBody=url
    )
