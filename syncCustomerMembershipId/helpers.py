correlationQuery = """query MyQuery($customerId: Int!) {
  admin_customer_correlation(where: {customer_id: {_eq: $customerId}}) {
    daily_customer_id
    adhoc_customer_id
    crm_customer_id
    hq_customer_id
  }
}
"""

customerMembershipInsert = """mutation MyMutation($customer_id: Int!, $membership_id: String!) {
  insert_customer_membership(objects: {customer_id: $customer_id, membership_id: $membership_id}) {
    affected_rows
  }
}
"""
