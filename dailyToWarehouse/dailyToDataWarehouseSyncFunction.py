import json
import logging
import os
import urllib3
import helpers
from collections import namedtuple


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
    logger.info(event)
    body = json.loads(event.get('body'))
    tableName = body['table']['name']
    if tableName == 'customer':
        postCustomerDataIntoWarehouse(body)
    else:
        postAddressIntoWarehouse(body)

    return {
        'statusCode': 200,
        'body': json.dumps("Event processed")
    }


def postCustomerDataIntoWarehouse(body):
    if body['event']['op'] == 'INSERT':
        return
    email = body['event']['data']['new']['primary_email']
    email = email.lower()
    warehouseCustomerIdVariables = {"email": email}
    warehouseCustomerIdBody = {
        'query': helpers.warehouseCustomerIdQuery, 'variables': warehouseCustomerIdVariables}
    response = postData(warehouseGraphQlUrl,
                        warehouseCustomerIdBody, WAREHOUSE_HEADERS)
    logger.info(response)
    if len(response['data']['admin_customer']) > 0:
        # customer already exist
        flag = 0
        warehouseCustomerId = response['data']['admin_customer'][0]['customer_id']
    else:
        # new customer
        flag = 1

    if flag == 1:
        # customer table insert
        dailyCustomerId = body['event']['data']['new']['id']
        firstName = body['event']['data']['new']['first_name']
        lastName = body['event']['data']['new']['last_name']
        email = body['event']['data']['new']['primary_email']
        email = email.lower()
        phoneNumber = body['event']['data']['new']['primary_phone']

        customerTableInsertVariables = {
            "first_name": firstName, "last_name": lastName, "primary_email": email, "primary_phone": phoneNumber}
        customerTableInsertBody = {
            'query': helpers.customerTableInsertQuery, 'variables': customerTableInsertVariables}
        response = postData(warehouseGraphQlUrl,
                            customerTableInsertBody, WAREHOUSE_HEADERS)
        logger.info("Customer inserted")

        warehouseCustomerIdVariables = {"email": email}
        warehouseCustomerIdBody = {
            'query': helpers.warehouseCustomerIdQuery, 'variables': warehouseCustomerIdVariables}
        response = postData(warehouseGraphQlUrl,
                            warehouseCustomerIdBody, WAREHOUSE_HEADERS)
        warehouseCustomerId = response['data']['admin_customer'][0]['customer_id']

        # customer address insert
        dailyCustomerId = body['event']['data']['new']['id']
        addressInsertIntoWarehouse(dailyCustomerId, warehouseCustomerId)

        # correlation table insert
        warehouseCorrelationInsertVariables = {
            "customer_id": warehouseCustomerId, "daily_customer_id": dailyCustomerId}
        warehouseCorrelationInsertBody = {
            'query': helpers.warehouseCorrelationInsertQuery, 'variables': warehouseCorrelationInsertVariables}
        response = postData(warehouseGraphQlUrl,
                            warehouseCorrelationInsertBody, WAREHOUSE_HEADERS)

    if flag == 0:
        # customer table update
        dailyCustomerId = body['event']['data']['new']['id']
        firstName = body['event']['data']['new']['first_name']
        lastName = body['event']['data']['new']['last_name']
        email = body['event']['data']['new']['primary_email']
        email = email.lower()
        phoneNumber = body['event']['data']['new']['primary_phone']

        customerTableUpdateVariables = {"eq": email, "first_name": firstName,
                                        "last_name": lastName, "primary_email": email, "primary_phone": phoneNumber}
        customerTableUpdateBody = {
            'query': helpers.customerTableUpdateQuery, 'variables': customerTableUpdateVariables}
        response = postData(warehouseGraphQlUrl,
                            customerTableUpdateBody, WAREHOUSE_HEADERS)
        logger.info("Customer Table updated")

        # address table update
        source = 'daily'
        addressTableDeleteVariables = {
            "customerId": warehouseCustomerId, "source": source}
        addressTableDeleteBody = {
            'query': helpers.addressTableDeleteQuery, 'variables': addressTableDeleteVariables}
        response = postData(warehouseGraphQlUrl,
                            addressTableDeleteBody, WAREHOUSE_HEADERS)

        addressInsertIntoWarehouse(dailyCustomerId, warehouseCustomerId)

        # correlation table update
        warehouseCorrelationUpdateVariables = {
            "customer_id": warehouseCustomerId, "daily_customer_id": dailyCustomerId}
        warehouseCorrelationUpdateBody = {
            'query': helpers.warehouseCorrelationUpdateQuery, 'variables': warehouseCorrelationUpdateVariables}
        response = postData(warehouseGraphQlUrl,
                            warehouseCorrelationUpdateBody, WAREHOUSE_HEADERS)
        logger.info("Correlation {}".format(response))


def postAddressIntoWarehouse(body):
    dailyCustomerId = body['event']['data']['new']['customer_id']
    customerCorrelationVariables = {"dailyCustomerId": dailyCustomerId}
    customerCorrelationBody = {
        'query': helpers.customerCorrelationQuery, 'variables': customerCorrelationVariables}
    response = postData(warehouseGraphQlUrl,
                        customerCorrelationBody, WAREHOUSE_HEADERS)
    warehouseCustomerId = response['data']['admin_customer_correlation'][0]['customer_id']

    source = 'daily'
    addressTableDeleteVariables = {
        "customerId": warehouseCustomerId, "source": source}
    addressTableDeleteBody = {
        'query': helpers.addressTableDeleteQuery, 'variables': addressTableDeleteVariables}
    response = postData(warehouseGraphQlUrl,
                        addressTableDeleteBody, WAREHOUSE_HEADERS)

    addressInsertIntoWarehouse(dailyCustomerId, warehouseCustomerId)


def postData(url, body, header):
    encoded_data = ""
    if len(body) > 0:
        encoded_data = json.dumps(body).encode('utf-8')

    request = http.request('POST', url, body=encoded_data, headers=header)
    response = json.loads(request.data.decode('utf-8'))
    return response


def addressInsertIntoWarehouse(dailyCustomerId, warehouseCustomerId):
    customerAddressVariables = {"customerId": dailyCustomerId}
    customerAddressBody = {
        'query': helpers.customerAddressQuery, 'variables': customerAddressVariables}
    response = postData(dailyGraphQlUrl, customerAddressBody, DAILY_HEADERS)

    address1 = response['data']['customer_address_view'][0]['flat']
    address2 = response['data']['customer_address_view'][0]['name']
    address3 = response['data']['customer_address_view'][0]['street']
    city = response['data']['customer_address_view'][0]['district']
    stateCode = response['data']['customer_address_view'][0]['state_code']
    countryCode = response['data']['customer_address_view'][0]['country_code']
    pincode = response['data']['customer_address_view'][0]['pincode']
    source = 'daily'

    customerAddressInsertVariables = {"address_1": address1, "address_2": address2, "address_3": address3, "city": city,
                                      "country_code": countryCode, "customer_id": warehouseCustomerId, "pincode": pincode, "source": source, "state_code": stateCode}
    customerAddressInsertBody = {
        'query': helpers.customerAddressInsertQuery, 'variables': customerAddressInsertVariables}
    response = postData(warehouseGraphQlUrl,
                        customerAddressInsertBody, WAREHOUSE_HEADERS)
    logger.info("Address inserted")
