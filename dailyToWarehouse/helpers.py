customerTableInsertQuery = """mutation MyMutation($primary_email: String!, $last_name: String!, $first_name: String!) {
  insert_admin_customer(objects: {first_name: $first_name, last_name: $last_name, primary_email: $primary_email}) {
    affected_rows
  }
}
"""

customerAddressQuery = """query MyQuery($customerId: Int!) {
  customer_address_view(where: {customer_id: {_eq: $customerId}}) {
    country_code
    district
    flat
    name
    pincode
    state_code
    street
  }
}"""

warehouseCustomerIdQuery = """query MyQuery($email: String!) {
  admin_customer(where: {primary_email: {_eq: $email}}) {
    customer_id
  }
}
"""

customerAddressInsertQuery = """mutation MyMutation($address_1: String!, $address_2: String!, $address_3: String!, $city: String!, $country_code: String!, $customer_id: Int!, $pincode: Int!, $source: String! , $state_code: String!,$phone:String!) {
  insert_admin_customer_contact(objects: {address_1: $address_1, address_2: $address_2, address_3: $address_3, city: $city, country_code: $country_code, customer_id: $customer_id, pincode: $pincode, source: $source, state_code: $state_code,phone:$phone}) {
    affected_rows
  }
}"""

customerTableUpdateQuery = """mutation MyMutation($first_name: String!, $last_name: String!, $primary_email: String!, $eq: String!) {
  update_admin_customer(where: {primary_email: {_eq: $eq}}, _set: {first_name: $first_name, last_name: $last_name, primary_email: $primary_email}) {
    affected_rows
  }
}
"""

addressTableDeleteQuery = """mutation MyMutation($customerId: Int!, $source: String!) {
  delete_admin_customer_contact(where: {source: {_eq: $source}, _and: {customer_id: {_eq: $customerId}}}) {
    affected_rows
  }
}
"""

warehouseCorrelationInsertQuery = """mutation MyMutation($customer_id: Int!, $daily_customer_id: Int!,$updated_at:timestamptz!) {
  insert_admin_customer_correlation(objects: {customer_id: $customer_id, daily_customer_id: $daily_customer_id,updated_at:$updated_at}) {
    affected_rows
  }
}"""


warehouseCorrelationUpdateQuery = """mutation MyMutation($customer_id: Int!, $daily_customer_id: Int!,$updated_at:timestamptz!) {
  update_admin_customer_correlation(where: {customer_id: {_eq: $customer_id}}, _set: {daily_customer_id: $daily_customer_id,updated_at:$updated_at}) {
    affected_rows
  }
}
"""

customerCorrelationQuery = """query MyQuery($dailyCustomerId: Int!) {
  admin_customer_correlation(where: {daily_customer_id: {_eq: $dailyCustomerId}}) {
    customer_id
  }
}
"""

addressTableUpdateQuery = """mutation MyMutation($_eq: String!, $state_code: String!, $pincode: Int!, $country_code: String!,$address_2:String!, $city: String!, $address_3: String!, $address_1: String!) {
  update_admin_customer_contact(where: {source: {_eq: $_eq}}, _set: {address_1: $address_1, address_2:$address_2, address_3: $address_3, city: $city, country_code: $country_code, pincode: $pincode, state_code: $state_code}) {
    affected_rows
  }
}
"""
