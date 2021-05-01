import json
import logging
import os
import urllib3
import helpers
from collections import namedtuple
from datetime import datetime
import requests

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
    # TODO implement
    logger.info("New customer sync to zoho invoked by a lambda...")
    logger.info(event)
    body = json.loads(event.get('body'))
    customerId = body['event']['data']['new']['customer_id']

    if customerId is None:
        logger.error(
            "Invalid customer id. Expecting a value but none received")
        return
    logger.info("Customer id to process " + str(customerId))
    customerInfo = getCustomerInfoFromWarehouse(customerId)

    if customerInfo is None:
        return

    logging.info(customerInfo)

    postCustomerInfoToCRM(customerInfo)

    return


def getCustomerInfoFromWarehouse(customerId):
    customerInfoVariables = {"customer_id": customerId}
    customerInfoBody = {'query': helpers.customerInfoQuery,
                        'variables': customerInfoVariables}
    response = postData(warehouseGraphQlUrl, customerInfoBody, HEADERS)

    return response


def getZohoAccessToken():
    clientId = os.environ['zoho_client_id']
    clientSecret = os.environ['zoho_client_secret']
    refreshToken = os.environ['zoho_refresh_token']
    refreshTokenApi = os.environ['zoho_token_api']

    if clientId is None:
        logger.error(
            "zoho client id could not be read from environment settings")
        return None

    if clientSecret is None:
        logger.error(
            "zoho client secret could not be read from environment settings")
        return None

    if refreshToken is None:
        logger.error(
            "zoho refresh token could not be read from environment settings")
        return None

    if refreshTokenApi is None:
        logger.error(
            "zoho refresh token API path could not be read from environment settings")
        return None

    logger.info("Requesting zoho access token")

    apiUrl = refreshTokenApi.format(
        refresh_token=refreshToken, client_id=clientId, client_secret=clientSecret)
    try:
        response = postData(apiUrl, "", "")

        # print(response) #DO NOT LOG OR PRINT ACCESS TOKEN. USE ONLY FOR DEBUGGING
        logger.info("Zoho access token received successfully")
        logger.info(response)
        return response["access_token"]

    except Exception as e:
        logger.error(e)
        raise e


def postCustomerInfoToCRM(customerInfo):
    logger.info("Reading environment variables for Zoho CRM...")
    accountsApi = os.environ['zoho_accounts_api']
    contactsApi = os.environ['zoho_contacts_api']

    accessToken = getZohoAccessToken()

    if accessToken is None:
        logger.error("Invalid access token or the access token is empty")
        return

    accountId = ""  # to be used for linking contact with the account

    # 1 save Account information to zoho
    try:
        logging.info("Creating an Account for the customer in zoho CRM")

        header = {'Content-Type': 'application/json',
                  'Authorization': 'Zoho-oauthtoken ' + accessToken}

        accountsPayload = getAccountsPayload(customerInfo)

        response = postData(accountsApi, accountsPayload, header)
        print(response)

        if 'code' in response:  # the response contains "code" dictionary key in case of an error, else the response contains 'data'
            logging.error("Failed to create account in zoho CRM")
            logging.error(response)
            return

        # retrieve the account id from the response which is used for associating contact with the account
        accountId = response["data"][0]["details"]["id"]
        logger.info(
            "Account created successfully in Zoho CRM with ID " + accountId)

    except Exception as e:
        logger.error(e)
        raise e

    # update the correlation table in the warehouse
    customerId = customerInfo['data']['admin_customer'][0]['customer_id']
    updateCorrelationTable(customerId, accountId)

    # 2 save Contact information to zoho

    contactsPayload = postContacts(customerInfo, accountId, accessToken)


def getAccountsPayload(customerInfo):
    if len(customerInfo['data']['admin_customer'][0]['address']) > 0:
        if 'phone' in customerInfo['data']['admin_customer'][0]['address'][0]:
            phoneNumber = customerInfo['data']['admin_customer'][0]['address'][0]['phone']
        else:
            phoneNumber = ""
        flag = 0
        if customerInfo['data']['admin_customer'][0]['address'][0]['address_2'] != None:
            address = customerInfo['data']['admin_customer'][0]['address'][0]['address_1'] + \
                customerInfo['data']['admin_customer'][0]['address'][0]['address_2']
            flag = 1
        if customerInfo['data']['admin_customer'][0]['address'][0]['address_3'] != None and flag == 1:
            address = address + \
                customerInfo['data']['admin_customer'][0]['address'][0]['address_3']
            flag = 1
        elif customerInfo['data']['admin_customer'][0]['address'][0]['address_3'] != None and flag == 0:
            address = customerInfo['data']['admin_customer'][0]['address'][0]['address_1'] + \
                customerInfo['data']['admin_customer'][0]['address'][0]['address_3']
            flag = 1
        if flag == 0:
            address = customerInfo['data']['admin_customer'][0]['address'][0]['address_1']
        accountsPayload = {
            "data": [
                {
                    "Phone": str(phoneNumber),
                    "Account_Name": customerInfo['data']['admin_customer'][0]['primary_email'],
                    "Account_Type": "Customer",
                    "Billing_Street": address,
                    "Billing_City": customerInfo['data']['admin_customer'][0]['address'][0]['city'],
                    "Billing_State": customerInfo['data']['admin_customer'][0]['address'][0]['state_code'],
                    "Billing_Country": customerInfo['data']['admin_customer'][0]['address'][0]['country_code'],
                    "Billing_Code": str(customerInfo['data']['admin_customer'][0]['address'][0]['pincode']),

                    "Shipping_Street": address,
                    "Shipping_City": customerInfo['data']['admin_customer'][0]['address'][0]['city'],
                    "Shipping_State": customerInfo['data']['admin_customer'][0]['address'][0]['state_code'],
                    "Shipping_Country": customerInfo['data']['admin_customer'][0]['address'][0]['country_code'],
                    "Shipping_Code": str(customerInfo['data']['admin_customer'][0]['address'][0]['pincode']),
                    "Lead_Source": str(customerInfo['data']['admin_customer'][0]['address'][0]['source'])
                }
            ],
            "duplicate_check_fields": [
                "Account_Name"
            ]
        }

        logger.info("Account payload created")
        logger.info(accountsPayload)
    else:
        accountsPayload = {
            "data": [
                {
                    "Phone": "",
                    "Account_Name": customerInfo['data']['admin_customer'][0]['primary_email'],
                    "Account_Type": "Customer",
                    "Billing_Street": "",
                    "Billing_City": "",
                    "Billing_State": "",
                    "Billing_Country": "",
                    "Billing_Code": "",

                    "Shipping_Street": "",
                    "Shipping_City": "",
                    "Shipping_State": "",
                    "Shipping_Country": "",
                    "Shipping_Code": "",
                    "Lead_Source": ""
                }
            ],
            "duplicate_check_fields": [
                "Account_Name"
            ]
        }
        logger.info("Account payload created")
        logger.info(accountsPayload)

    return accountsPayload


def postContacts(customerInfo, accountId, accessToken):
    if len(customerInfo['data']['admin_customer'][0]['address']) > 0:
        for i in range(0, len(customerInfo['data']['admin_customer'][0]['address'])):
            if 'phone' in customerInfo['data']['admin_customer'][0]['address'][i]:
                phoneNumber = customerInfo['data']['admin_customer'][0]['address'][i]['phone']
            else:
                phoneNumber = ""
            flag = 0
            if customerInfo['data']['admin_customer'][0]['address'][i]['address_2'] != None:
                address = customerInfo['data']['admin_customer'][0]['address'][i]['address_1'] + \
                    customerInfo['data']['admin_customer'][0]['address'][i]['address_2']
                flag = 1
            if customerInfo['data']['admin_customer'][0]['address'][i]['address_3'] != None and flag == 1:
                address = address + \
                    customerInfo['data']['admin_customer'][0]['address'][i]['address_3']
                flag = 1
            elif customerInfo['data']['admin_customer'][0]['address'][i]['address_3'] != None and flag == 0:
                address = customerInfo['data']['admin_customer'][0]['address'][i]['address_1'] + \
                    customerInfo['data']['admin_customer'][0]['address'][i]['address_3']
                flag = 1
            if flag == 0:
                address = customerInfo['data']['admin_customer'][0]['address'][i]['address_1']
            if 'date_of_birth' in customerInfo['data']['admin_customer'][0]:
                dateOfBirth = customerInfo['data']['admin_customer'][0]['date_of_birth']
            contactsPayload = {
                "data": [
                    {
                        "Email": customerInfo['data']['admin_customer'][0]['primary_email'],
                        "Mailing_Street": address,
                        "Mailing_City": customerInfo['data']['admin_customer'][0]['address'][i]['city'],
                        "Mailing_State": customerInfo['data']['admin_customer'][0]['address'][i]['state_code'],
                        "Mailing_Zip": str(customerInfo['data']['admin_customer'][0]['address'][i]['pincode']),
                        "Mailing_Country": customerInfo['data']['admin_customer'][0]['address'][i]['country_code'],
                        "First_Name": customerInfo['data']['admin_customer'][0]['first_name'],
                        "Last_Name": customerInfo['data']['admin_customer'][0]['last_name'],
                        "Full_Name":  customerInfo['data']['admin_customer'][0]['first_name'] + " " + customerInfo['data']['admin_customer'][0]['last_name'],
                        "Lead_Source": str(customerInfo['data']['admin_customer'][0]['address'][i]['source']),
                        "Phone": str(phoneNumber),
                        "Date_of_Birth":dateOfBirth,
                        "Account_Name": {
                            "name": customerInfo['data']['admin_customer'][0]['primary_email'],
                            "id": accountId
                        },
                        "Mobile": phoneNumber
                    }
                ],
                "duplicate_check_fields": [
                    "Email"
                ]
            }

            logger.info("Contacts payload created")
            logger.info(contactsPayload)
            header = {"Content-Type": "application/json",
                      "Authorization": "Zoho-oauthtoken " + accessToken}
            contactsApi = os.environ['zoho_contacts_api']
            response = postData(contactsApi, contactsPayload, header)
            logger.info(response)
            if 'code' in response:  # the response contains "code" dictionary key in case of an error, else the response contains 'data'
                logging.error("Failed to create contact in zoho CRM")
                logging.error(response)
                return
            if response["data"][0]["status"] == "error":
                logging.error("Failed to create contact in zoho CRM")
                logging.error(response)
                return
            contactId = response["data"][0]["details"]["id"]
            logger.info(
                "Contact created successfully in Zoho CRM with ID " + contactId)

    else:
        contactsPayload = {
            "data": [
                {
                    "Email": customerInfo['data']['admin_customer'][0]['primary_email'],
                    "Mailing_Street":"",
                    "Mailing_City": "",
                    "Mailing_State": "",
                    "Mailing_Zip": "",
                    "Mailing_Country": "",
                    "First_Name": customerInfo['data']['admin_customer'][0]['first_name'],
                    "Last_Name": customerInfo['data']['admin_customer'][0]['last_name'],
                    "Full_Name":  customerInfo['data']['admin_customer'][0]['first_name'] + " " + customerInfo['data']['admin_customer'][0]['last_name'],
                    "Phone": "",
                    "Account_Name": {
                        "name": customerInfo['data']['admin_customer'][0]['primary_email'],
                        "id": accountId
                    },
                    "Mobile": ""
                }
            ],
            "duplicate_check_fields": [
                "Email"
            ]
        }

        logger.info("Contacts payload created")
        logger.info(contactsPayload)
        header = {"Content-Type": "application/json",
                  "Authorization": "Zoho-oauthtoken " + accessToken}
        contactsApi = os.environ['zoho_contacts_api']
        response = postData(contactsApi, contactsPayload, header)
        logger.info(response)

        if 'code' in response:  # the response contains "code" dictionary key in case of an error, else the response contains 'data'
            logging.error("Failed to create contact in zoho CRM")
            logging.error(response)
            return

        if response["data"][0]["status"] == "error":
            logging.error("Failed to create contact in zoho CRM")
            logging.error(response)
            return

        contactId = response["data"][0]["details"]["id"]
        logger.info(
            "Contact created successfully in Zoho CRM with ID " + contactId)


def updateCorrelationTable(customerId, accountId):
    updatedAt = datetime.now()
    correlationTableUpdateVaraibles = {
        "customer_id": customerId, "crm_customer_id": accountId, "updated_at": updatedAt}
    correlationTableUpdateBody = {
        'query': helpers.correlationTableUpdateQuery, 'variables': correlationTableUpdateVaraibles}
    response = postData(warehouseGraphQlUrl,
                        correlationTableUpdateBody, HEADERS)
    logger.info("customer correlation updated")
    logger.info(response)


def postData(url, body, header):
    body = json.dumps(body, default=myconverter)
    request = requests.post(url, data=body, headers=header)
    response = request.json()
    return response


def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()
