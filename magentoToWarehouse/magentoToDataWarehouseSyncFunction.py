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

warehouseGraphQlEndpoint = os.environ['GRAPHQL_ENDPOINT']
warehouseGraphQlUrl = warehouseGraphQlEndpoint + '/v1/graphql'
accessKey = os.environ['GRAPHQL_ADMIN_SECRET']
HEADERS = {
    'Content-Type': 'application/json',
    'X-Hasura-Access-Key': accessKey,
}


def lambda_handler(event, context):
    logger.info(event)
    global env

    logger.info("New customer sync to warehouse invoked by a lambda...")
    customerId = event["customerId"]
    env = event["env"]

    if customerId is None:
        logger.error(
            "Invalid customer id. Expecting a value but none received")
        return

    if env is None:
        logger.error(
            "Invalid env. Expecting dev/staging/prod but none received")
        return

    logger.info("Customer id to process " + customerId)

    customerInfo = getCustomerInfoFromMagento(customerId)

    if customerInfo is None:
        return

    logging.info(customerInfo)

    customerExists = validateIfCustomerExistsInWarehouse(customerInfo)
    postCustomerInfoToWarehouse(customerInfo, customerExists)

    return {
        'statusCode': 200,
        'body': json.dumps("Event processed")
    }


def getCustomerInfoFromMagento(customerId):

    logger.info("Reading environment variables for magento...")
    magentoAuthToken = ""
    magentoRESTApi = ""

    if env == "prod":
        magentoAuthToken = os.environ['magento_auth_token']
        magentoRESTApi = os.environ['magento_customer_rest_api']

    elif env == "dev":
        magentoAuthToken = os.environ['magento_dev_auth_token']
        magentoRESTApi = os.environ['magento_dev_customer_rest_api']
    else:
        magentoAuthToken = os.environ['magento_staging_auth_token']
        magentoRESTApi = os.environ['magento_staging_customer_rest_api']

    if magentoAuthToken is None or len(magentoAuthToken.strip()) == 0:
        logging.error(
            "Could not find magento auth token in environment settings")
        return None

    if magentoRESTApi is None or len(magentoRESTApi.strip()) == 0:
        logging.error(
            "Could not find magento REST api path in environment settings")
        return None

    apiUrl = magentoRESTApi + customerId
    header = {"Authorization": "Bearer {}".format(
        magentoAuthToken), 'Content-Type': 'application/json'}

    logging.info("Requesting magento REST api for customer information with id " +
                 customerId + " at " + apiUrl + " ...")

    try:
        request = http.request('GET', apiUrl, headers=header)
        response = json.loads(request.data.decode('utf-8'))

        logger.info("Customer information received successfully " +
                    str(response["id"]))
        return response

    except Exception as e:
        logger.error(e)
        raise e


def postCustomerInfoToWarehouse(customerInfo, customerExists):
    logger.info(customerExists)
    if customerExists == False:
        # insert into customer table
        customerId = customerInfo['id']
        firstName = customerInfo['firstname']
        lastName = customerInfo['lastname']
        email = customerInfo['email']
        email = email.lower()
        if 'gender' in customerInfo:
            gender = str(customerInfo['gender'])
        else:
            gender = ""
        customerInsertVariables = {
            "first_name": firstName, "gender": gender, "last_name": lastName, "primary_email": email}
        if 'dob' in customerInfo:
            dateOfBirth = customerInfo['dob']
            customerInsertVariables['date_of_birth'] = dateOfBirth

        customerInsertBody = {
            'query': helpers.customerTableInsertQuery, 'variables': customerInsertVariables}
        response = postData(warehouseGraphQlUrl, customerInsertBody, HEADERS)
        logger.info("Customer Data for customer ID " +
                    str(customerId) + " uploaded in Warehouse")

        #query Warehouse customerId
        email = customerInfo['email']
        email = email.lower()
        customerCorrelationVariables = {"primary_email": email}
        customerCorrelationBody = {
            'query': helpers.customerIdQuery, 'variables': customerCorrelationVariables}
        response = postData(warehouseGraphQlUrl,
                            customerCorrelationBody, HEADERS)
        warehouseCustomerId = response['data']['admin_customer'][0]['customer_id']

        #insert into correlation table
        adhocCustomerId = customerInfo['id']
        updatedAt = datetime.now()
        customerCorrelationInsertVariables = {
            "adhoc_customer_id": adhocCustomerId, "customer_id": warehouseCustomerId, "updated_at": updatedAt}
        customerCorrelationInsertBody = {
            'query': helpers.customerCorrelationInsertQuery, 'variables': customerCorrelationInsertVariables}
        response = postData(warehouseGraphQlUrl,
                            customerCorrelationInsertBody, HEADERS)
        logger.info("Correlation table updated in warehouse")

        #insert into address table
        if len(customerInfo['addresses']) > 0:
            for i in range(len(customerInfo['addresses'])):
                address = customerInfo['addresses'][i]['street'][0]
                city = customerInfo['addresses'][i]['city']
                stateCode = customerInfo['addresses'][i]['region']['region_code']
                countryCode = customerInfo['addresses'][i]['country_id']
                pincode = int(customerInfo['addresses'][i]['postcode'])
                phoneNumber = customerInfo['addresses'][i]['telephone']
                source = 'adhoc'
                customerContactTableInsertVariables = {"address_1": address, "city": city, "country_code": countryCode,
                                                       "customer_id": warehouseCustomerId, "phone": phoneNumber, "pincode": pincode, "source": source, "state_code": stateCode}
                customerContactTableInsertBody = {
                    'query': helpers.customerContactTableInsertQuery, 'variables': customerContactTableInsertVariables}
                response = postData(warehouseGraphQlUrl,
                                    customerContactTableInsertBody, HEADERS)

    else:
        # checking if any field has to be updated
        email = customerInfo['email']
        email = email.lower()
        validationQueryVariables = {"primary_email": email}
        validationQueryBody = {
            'query': helpers.validationQuery, 'variables': validationQueryVariables}
        response = postData(warehouseGraphQlUrl, validationQueryBody, HEADERS)
        warehouseCustomerId = response['data']['admin_customer'][0]['customer_id']
        tempDateOfBirth = response['data']['admin_customer'][0]['date_of_birth']
        tempGender = response['data']['admin_customer'][0]['gender']
        if 'dob' in customerInfo:
            dateOfBirth = customerInfo['dob']
            customerUpdateVariables = {
                "email": email, "date_of_birth": dateOfBirth}
            customerUpdateBody = {
                'query': helpers.customerDobUpdateQuery, 'variables': customerUpdateVariables}
            response = postData(warehouseGraphQlUrl,
                                customerUpdateBody, HEADERS)
        if 'gender' in customerInfo:
            if customerInfo['gender'] == 1:
                gender = 'Male'
            if customerInfo['gender'] == 2:
                gender = 'Female'
            if customerInfo['gender'] == 3:
                gender = 'Not specified'
        else:
            gender = ""

            customerUpdateVariables = {"email": email, "gender": gender}
            customerUpdateBody = {
                'query': helpers.customerGenderUpdateQuery, 'variables': customerUpdateVariables}
            response = postData(warehouseGraphQlUrl,
                                customerUpdateBody, HEADERS)

        # update in correlation table
        adhocCustomerId = customerInfo['id']
        updatedAt = datetime.now()
        customerCorrelationUpdateVariables = {
            "customer_id": warehouseCustomerId, "adhoc_customer_id": adhocCustomerId, "updated_at": updatedAt}
        customerCorrelationUpdateBody = {
            'query': helpers.customerCorrelationUpdateQuery, 'variables': customerCorrelationUpdateVariables}
        response = postData(warehouseGraphQlUrl,
                            customerCorrelationUpdateBody, HEADERS)
        logger.info("Correlation table updated in the warehouse")

        #deleting the existing customer address and updating new one
        source = 'adhoc'
        customerContactDeleteVariables = {
            "source": source, "customer_id": warehouseCustomerId}
        customerContactDeleteBody = {
            'query': helpers.customerContactDeleteQuery, 'variables': customerContactDeleteVariables}
        response = postData(warehouseGraphQlUrl,
                            customerContactDeleteBody, HEADERS)
        if len(customerInfo['addresses']) > 0:
            for i in range(len(customerInfo['addresses'])):
                address = customerInfo['addresses'][i]['street'][0]
                city = customerInfo['addresses'][i]['city']
                stateCode = customerInfo['addresses'][i]['region']['region_code']
                countryCode = customerInfo['addresses'][i]['country_id']
                phoneNumber = customerInfo['addresses'][i]['telephone']
                pincode = int(customerInfo['addresses'][i]['postcode'])
                source = 'adhoc'
                customerContactTableInsertVariables = {"address_1": address, "city": city, "country_code": countryCode,
                                                       "customer_id": warehouseCustomerId, "phone": phoneNumber,  "pincode": pincode, "source": source, "state_code": stateCode}
                customerContactTableInsertBody = {
                    'query': helpers.customerContactTableInsertQuery, 'variables': customerContactTableInsertVariables}
                response = postData(warehouseGraphQlUrl,
                                    customerContactTableInsertBody, HEADERS)


def postData(url, body, header):
    encoded_data = ""
    if len(body) > 0:
        encoded_data = json.dumps(body, default=myconverter).encode('utf-8')

    request = http.request('POST', url, body=encoded_data, headers=header)
    response = json.loads(request.data.decode('utf-8'))
    return response


def validateIfCustomerExistsInWarehouse(customerInfo):
    email = customerInfo['email']
    email = email.lower()
    customerValidationVariables = {"primary_email": email}
    customerValidationVariablesBody = {
        'query': helpers.customerIdQuery, 'variables': customerValidationVariables}
    response = postData(warehouseGraphQlUrl,
                        customerValidationVariablesBody, HEADERS)
    queryResponse = response['data']['admin_customer']
    if len(queryResponse) > 0:
        return True
    else:
        return False


def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()
