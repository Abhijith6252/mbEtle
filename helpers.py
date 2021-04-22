customerTableInsertQuery="""mutation MyMutation($date_of_birth: date, $first_name: String!, $gender: String!, $last_name: String!, $primary_email: String!, $primary_phone: String!) {
  insert_admin_customer(objects: {date_of_birth: $date_of_birth, first_name: $first_name, gender: $gender, last_name: $last_name, primary_email: $primary_email, primary_phone: $primary_phone}){
  affected_rows
}}"""


customerIdQuery="""query MyQuery($primary_email: String!) {
  admin_customer(where: {primary_email: {_eq: $primary_email}}) {
    customer_id
  }
}"""

customerCorrelationInsertQuery="""mutation MyMutation($adhoc_customer_id: Int!, $customer_id: Int!) {
  insert_admin_customer_correlation(objects: {adhoc_customer_id: $adhoc_customer_id, customer_id: $customer_id}) {
    affected_rows
  }
}
"""

validationQuery="""query MyQuery($primary_email: String!) {
  admin_customer(where: {primary_email: {_eq: $primary_email}}) {
    customer_id
    gender
    date_of_birth
    primary_phone
  }
}"""


customerCorrelationUpdateQuery="""mutation MyMutation($customer_id: Int!, $adhoc_customer_id: Int!) {
  update_admin_customer_correlation(where: {customer_id: {_eq: $customer_id}}, _set: {adhoc_customer_id: $adhoc_customer_id}) {
    affected_rows
  }
}
"""

customerDobUpdateQuery="""mutation MyMutation($email: String!, $date_of_birth: date!) {
  update_admin_customer(where: {primary_email: {_eq:$email}}, _set: {date_of_birth: $date_of_birth}) {
    affected_rows
  }
}
"""

customerGenderUpdateQuery="""mutation MyMutation($email: String!, $gender: String!) {
  update_admin_customer(where: {primary_email: {_eq:$email}}, _set: {gender: $gender}) {
    affected_rows
  }
}
"""

customerPhoneUpdateQuery="""mutation MyMutation($email: String!,$primary_phone: String) {
  update_admin_customer(where: {primary_email: {_eq:$email}}, _set: {primary_phone: $primary_phone}) {
    affected_rows
  }
}
"""

customerPhoneValidationQuery="""query MyQuery($customer_id: Int!) {
  admin_customer_phone(where: {customer_id: {_eq: $customer_id}}) {
    customer_phone
  }
}
"""

customerPhoneTableInsertQuery="""mutation MyMutation($customer_id: Int!, $customer_phone: String!) {
  insert_admin_customer_phone(objects: {customer_id: $customer_id, customer_phone: $customer_phone}) {
    affected_rows
  }
}
"""

customerAddressTableInsertQuery="""mutation MyMutation($address_1: String!, $city: String!, $country_code: String!, $customer_id: Int!, $pincode: Int!, $source: String!, $state_code: String!) {
  insert_admin_customer_address(objects: {address_1: $address_1, city: $city, country_code: $country_code, customer_id: $customer_id, pincode: $pincode, source: $source, state_code: $state_code}) {
    affected_rows
  }
}
"""

customerAddressDeleteQuery="""mutation MyMutation($source: String!, $customer_id: Int!) {
  delete_admin_customer_address(where: {source: {_eq: $source}, _and: {customer_id: {_eq: $customer_id}}}) {
    affected_rows
  }
}
"""