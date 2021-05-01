import json
import logging
import os
import urllib3
import helpers
from collections import namedtuple
from datetime import datetime


logger = logging.getLogger()
logger.setLevel(logging.INFO)
http = urllib3.PoolManager()


dailyGraphQlEndpoint = os.environ['DAILY_GRAPHQL_ENDPOINT']
dailyGraphQlUrl = dailyGraphQlEndpoint+'/v1/graphql'
dailyAccessKey = os.environ['DAILY_GRAPHQL_ADMIN_SECRET']
DAILY_HEADERS = {
    'Content-Type': 'application/json',
    'X-Hasura-Access-Key': dailyAccessKey
}


warehouseGraphQlEndpoint = os.environ['WAREHOUSE_GRAPHQL_ENDPOINT']
warehouseGraphQlUrl = warehouseGraphQlEndpoint+'/v1/graphql'
warehouseAccessKey = os.environ['WAREHOUSE_GRAPHQL_ADMIN_SECRET']
WAREHOUSE_HEADERS = {
    'Content-Type': 'application/json',
    'X-Hasura-Access-Key': warehouseAccessKey
}


def lambda_handler(event, context):
    # TODO implement
    body = json.loads(event.get('body'))
    membershipId = body['event']['data']['new']['membership_id']
    customerId = body['event']['data']['new']['customer_id']

    correlationQueryVariables = {"customerId": customerId}
    correlationQueryBody = {
        'query': helpers.correlationQuery, 'variables': correlationQueryVariables}
    response = postData(warehouseGraphQlUrl,
                        correlationQueryBody, WAREHOUSE_HEADERS)

    logger.info(response)

    dailyCustomerId = response['data']['admin_customer_correlation'][0]['daily_customer_id']

    customerMembershipInsertVariables = {
        "customer_id": dailyCustomerId, "membership_id": membershipId}
    customerMembershipInsertBody = {
        'query': helpers.customerMembershipInsert, 'variables': customerMembershipInsertVariables}
    response = postData(
        dailyGraphQlUrl, customerMembershipInsertBody, DAILY_HEADERS)
    logger.info(response)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def postData(url, body, header):
    encoded_data = ""
    if len(body) > 0:
        encoded_data = json.dumps(body).encode('utf-8')

    request = http.request('POST', url, body=encoded_data, headers=header)
    response = json.loads(request.data.decode('utf-8'))
    return response
