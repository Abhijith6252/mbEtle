customerTableInsertQuery = """mutation MyMutation($primary_phone: String!, $primary_email: String!, $last_name: String!, $first_name: String!) {
  insert_admin_customer(objects: {first_name: $first_name, last_name: $last_name, primary_email: $primary_email, primary_phone: $primary_phone}) {
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

customerAddressInsertQuery = """mutation MyMutation($address_1: String = "", $address_2: String = "", $address_3: String = "", $city: String = "", $country_code: String = "", $customer_id: Int = 10, $pincode: Int = 10, $source: String = "", $state_code: String = "") {
  insert_admin_customer_address(objects: {address_1: $address_1, address_2: $address_2, address_3: $address_3, city: $city, country_code: $country_code, customer_id: $customer_id, pincode: $pincode, source: $source, state_code: $state_code}) {
    affected_rows
  }
}"""

customerTableUpdateQuery = """mutation MyMutation($first_name: String!, $last_name: String!, $primary_email: String!, $primary_phone: String!, $eq: String!) {
  update_admin_customer(where: {primary_email: {_eq: $eq}}, _set: {first_name: $first_name, last_name: $last_name, primary_email: $primary_email, primary_phone: $primary_phone}) {
    affected_rows
  }
}
"""

addressTableDeleteQuery = """mutation MyMutation($customerId: Int!, $source: String!) {
  delete_admin_customer_address(where: {source: {_eq: $source}, _and: {customer_id: {_eq: $customerId}}}) {
    affected_rows
  }
}
"""

warehouseCorrelationInsertQuery = """mutation MyMutation($customer_id: Int!, $daily_customer_id: Int!) {
  insert_admin_customer_correlation(objects: {customer_id: $customer_id, daily_customer_id: $daily_customer_id}) {
    affected_rows
  }
}"""


warehouseCorrelationUpdateQuery = """mutation MyMutation($customer_id: Int!, $daily_customer_id: Int!) {
  update_admin_customer_correlation(where: {customer_id: {_eq: $customer_id}}, _set: {daily_customer_id: $daily_customer_id}) {
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
