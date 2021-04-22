import json
import logging
import os
import urllib3
import helpers
from collections import namedtuple

logger = logging.getLogger()
logger.setLevel(logging.INFO)
http = urllib3.PoolManager()

warehouseGraphQlEndpoint = os.environ['GRAPHQL_ENDPOINT']
warehouseGraphQlUrl = warehouseGraphQlEndpoint+'/v1/graphql'
accessKey = os.environ['GRAPHQL_ADMIN_SECRET']
HEADERS = {
    'Content-Type': 'application/json',
    'X-Hasura-Access-Key': accessKey,
}


def lambda_handler(event, context):
    logger.info(event)
    global env

    logger.info("New customer sync to warehouse invoked by a lambda...")

    logger.info(event)
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
        # logger.info(str(response))

        return response

    except Exception as e:
        logger.error(e)
        raise e


def postCustomerInfoToWarehouse(customerInfo, customerExists):
    phoneNumberList = []
    if customerExists == False:
        # insert into customer table
        customerId = customerInfo['id']
        firstName = customerInfo['firstname']
        lastName = customerInfo['lastname']
        email = customerInfo['email']
        email = email.lower()
        phoneNumber = ""
        if len(customerInfo['addresses']) > 0:
            if 'telephone' in customerInfo['addresses'][0]:
                phoneNumber = customerInfo['addresses'][0]['telephone']

        if len(customerInfo['addresses']) > 1:
            for i in range(1, len(customerInfo['addresses'])):
                phoneNumberList.append(
                    customerInfo['addresses'][i]['telephone'])
        if 'gender' in customerInfo:
            gender = str(customerInfo['gender'])
        else:
            gender = ""
        customerInsertVariables = {"first_name": firstName, "gender": gender,
                                   "last_name": lastName, "primary_email": email, "primary_phone": phoneNumber}
        if 'dob' in customerInfo:
            dateOfBirth = customerInfo['dob']
            customerInsertVariables['date_of_birth'] = dateOfBirth

        customerInsertBody = {
            'query': helpers.customerTableInsertQuery, 'variables': customerInsertVariables}
        response = postData(warehouseGraphQlUrl, customerInsertBody, HEADERS)
        logger.info("Customer Data uploaded in Warehouse")

        # query Warehouse customerId
        email = customerInfo['email']
        email = email.lower()
        customerCorrelationVariables = {"primary_email": email}
        customerCorrelationBody = {
            'query': helpers.customerIdQuery, 'variables': customerCorrelationVariables}
        response = postData(warehouseGraphQlUrl,
                            customerCorrelationBody, HEADERS)
        warehouseCustomerId = response['data']['admin_customer'][0]['customer_id']

        # insert into correlation table
        adhocCustomerId = customerInfo['id']
        customerCorrelationInsertVariables = {
            "adhoc_customer_id": adhocCustomerId, "customer_id": warehouseCustomerId}
        customerCorrelationInsertBody = {
            'query': helpers.customerCorrelationInsertQuery, 'variables': customerCorrelationInsertVariables}
        response = postData(warehouseGraphQlUrl,
                            customerCorrelationInsertBody, HEADERS)
        logger.info("Correlation table updated")

        # insert into address table
        if len(customerInfo['addresses']) > 0:
            for i in range(len(customerInfo['addresses'])):
                address = customerInfo['addresses'][i]['street'][0]
                city = customerInfo['addresses'][i]['city']
                stateCode = customerInfo['addresses'][i]['region']['region_code']
                countryCode = customerInfo['addresses'][i]['country_id']
                pincode = int(customerInfo['addresses'][i]['postcode'])
                source = 'adhoc'
                customerAddressTableInsertVariables = {"address_1": address, "city": city, "country_code": countryCode,
                                                       "customer_id": warehouseCustomerId, "pincode": pincode, "source": source, "state_code": stateCode}
                customerAddressTableInsertBody = {
                    'query': helpers.customerAddressTableInsertQuery, 'variables': customerAddressTableInsertVariables}
                response = postData(warehouseGraphQlUrl,
                                    customerAddressTableInsertBody, HEADERS)
                logger.info(response)

    else:

        # checking if any field has to be updated
        email = customerInfo['email']
        email = email.lower()
        validationQueryVariables = {"primary_email": email}
        validationQueryBody = {
            'query': helpers.validationQuery, 'variables': validationQueryVariables}
        response = postData(warehouseGraphQlUrl, validationQueryBody, HEADERS)
        warehouseCustomerId = response['data']['admin_customer'][0]['customer_id']
        logger.info(response)
        tempDateOfBirth = response['data']['admin_customer'][0]['date_of_birth']
        tempGender = response['data']['admin_customer'][0]['gender']
        tempPhoneNumber = response['data']['admin_customer'][0]['primary_phone']
        if 'dob' in customerInfo:
            dateOfBirth = customerInfo['dob']
            customerUpdateVariables = {
                "email": email, "date_of_birth": dateOfBirth}
            customerUpdateBody = {
                'query': helpers.customerDobUpdateQuery, 'variables': customerUpdateVariables}
            response = postData(warehouseGraphQlUrl,
                                customerUpdateBody, HEADERS)
        if 'gender' in customerInfo:
            gender = str(customerInfo['gender'])
            customerUpdateVariables = {"email": email, "gender": gender}
            customerUpdateBody = {
                'query': helpers.customerGenderUpdateQuery, 'variables': customerUpdateVariables}
            response = postData(warehouseGraphQlUrl,
                                customerUpdateBody, HEADERS)
        if len(customerInfo['addresses']) > 0:
            if 'telephone' in customerInfo['addresses'][0]:
                phoneNumber = customerInfo['addresses'][0]['telephone']
                customerUpdateVariables = {
                    "email": email, "primary_phone": phoneNumber}
                customerUpdateBody = {
                    'query': helpers.customerPhoneUpdateQuery, 'variables': customerUpdateVariables}
                response = postData(warehouseGraphQlUrl,
                                    customerUpdateBody, HEADERS)

        # update in correlation table
        adhocCustomerId = customerInfo['id']
        customerCorrelationUpdateVariables = {
            "customer_id": warehouseCustomerId, "adhoc_customer_id": adhocCustomerId}
        customerCorrelationUpdateBody = {
            'query': helpers.customerCorrelationUpdateQuery, 'variables': customerCorrelationUpdateVariables}
        response = postData(warehouseGraphQlUrl,
                            customerCorrelationUpdateBody, HEADERS)
        logger.info("Correlation table updated")

        # getting list of phone numbers and inserting the ones that are not present already
        tempPhoneNumberList = []
        phoneNumberValidationList = []
        if len(customerInfo['addresses']) > 1:
            for i in range(1, len(customerInfo['addresses'])):
                tempPhoneNumberList.append(
                    customerInfo['addresses'][i]['telephone'])
            customerPhoneValidationVariables = {
                "customer_id": warehouseCustomerId}
            customerPhoneValidationBody = {
                'query': helpers.customerPhoneValidationQuery, 'variables': customerPhoneValidationVariables}
            response = postData(warehouseGraphQlUrl,
                                customerPhoneValidationBody, HEADERS)
            logger.info(response)
            if len(response['data']['admin_customer_phone']) > 0:
                for j in range(1, len(response['data']['admin_customer_phone'])):
                    phoneNumberValidationList.append(
                        response['data']['admin_customer_phone'][j]['customer_phone'])
                for number in tempPhoneNumberList:
                    if number not in phoneNumberValidationList:
                        phoneNumberList.append(number)
            else:
                phoneNumberList = tempPhoneNumberList

        # deleting the existing customer address and updating new one
        source = 'adhoc'
        customerAddressDeleteVariables = {
            "source": source, "customer_id": warehouseCustomerId}
        customerAddressDeleteBody = {
            'query': helpers.customerAddressDeleteQuery, 'variables': customerAddressDeleteVariables}
        response = postData(warehouseGraphQlUrl,
                            customerAddressDeleteBody, HEADERS)
        logger.info(customerAddressDeleteVariables)
        logger.info("old records deleted")
        logger.info(response)
        if len(customerInfo['addresses']) > 0:
            for i in range(len(customerInfo['addresses'])):
                address = customerInfo['addresses'][i]['street'][0]
                city = customerInfo['addresses'][i]['city']
                stateCode = customerInfo['addresses'][i]['region']['region_code']
                countryCode = customerInfo['addresses'][i]['country_id']
                pincode = int(customerInfo['addresses'][i]['postcode'])
                source = 'adhoc'
                customerAddressTableInsertVariables = {"address_1": address, "city": city, "country_code": countryCode,
                                                       "customer_id": warehouseCustomerId, "pincode": pincode, "source": source, "state_code": stateCode}
                customerAddressTableInsertBody = {
                    'query': helpers.customerAddressTableInsertQuery, 'variables': customerAddressTableInsertVariables}
                response = postData(warehouseGraphQlUrl,
                                    customerAddressTableInsertBody, HEADERS)
                logger.info(response)

    updateCustomerPhoneTable(phoneNumberList, warehouseCustomerId)


def updateCustomerPhoneTable(phoneNumberList, warehouseCustomerId):
    for number in phoneNumberList:
        customerPhoneTableInsertVariables = {
            "customer_id": warehouseCustomerId, "customer_phone": number}
        customerPhoneTableInsertBody = {
            'query': helpers.customerPhoneTableInsertQuery, 'variables': customerPhoneTableInsertVariables}
        response = postData(warehouseGraphQlUrl,
                            customerPhoneTableInsertBody, HEADERS)


def postData(url, body, header):
    encoded_data = ""
    if len(body) > 0:
        encoded_data = json.dumps(body).encode('utf-8')

    request = http.request('POST', url, body=encoded_data, headers=header)
    response = json.loads(request.data.decode('utf-8'))
    return response


def validateIfCustomerExistsInWarehouse(customerInfo):
    email = customerInfo['email']
    email = email.lower()
    logger.info(email)
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
