import boto3
import json
import logging
import datetime
import os
import urllib3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
http = urllib3.PoolManager()
client = boto3.client("lambda", region_name="ap-south-1")


def lambda_handler(event, context):
    # TODO implement
    if 'Records' in event:
        logger.info("A new SQS payload has arrived from magento webhook")
        logger.info(event)
        sqsPayload = event['Records'][0]['body']
        sqsPayload = sqsPayload.strip()
        logger.info(sqsPayload)
        sqsPayload = sqsPayload[1:-1]
        logger.info("Extracting payload details...")
        details = sqsPayload.split(",")
        entityId = details[0]
        env = details[2].lower()
        logger.info("Invoking magentoToDataWarehouseSyncFunction...")
        payload = {"customerId": entityId, "env": env}
        logger.info("The payload invoking lambda {}".format(payload))
        response = client.invoke(FunctionName="magentoToDataWarehouseSyncFunction",
                                 InvocationType="Event", Payload=json.dumps(payload))
        logger.info(response)
    else:
        logger.info("A new hasura payload has arrived from daily")
        logger.info("Invoking dailyToDataWarehouseSyncFunction...")
        payload = event
        logger.info("The payload invoking lambda {}".format(payload))
        response = client.invoke(FunctionName="dailyToDataWarehouseSyncFunction",
                                 InvocationType="Event", Payload=json.dumps(payload))
        logger.info(response)

    return {
        'statusCode': 200,
        'body': json.dumps("Event processed")
    }
