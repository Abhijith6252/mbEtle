
customerInfoQuery = """query MyQuery($customer_id: Int!) {
  admin_customer(where: {customer_id: {_eq: $customer_id}}) {
    date_of_birth
    first_name
    gender
    last_name
    primary_email
    customer_id
    address {
      address_1
      address_2
      address_3
      city
      country_code
      phone
      pincode
      state_code
      source
    }
  }
}"""

correlationTableUpdateQuery = """mutation MyMutation($customer_id: Int!, $crm_customer_id: bigint!, $updated_at: timestamptz!) {
  update_admin_customer_correlation(where: {customer_id: {_eq: $customer_id}}, _set: {crm_customer_id: $crm_customer_id, updated_at: $updated_at}) {
    affected_rows
  }
}
"""
