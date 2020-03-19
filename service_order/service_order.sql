For example:

(SELECT last_name || DECODE (first_name, NULL, NULL, ', ' || first_name)
   FROM ra_contacts
  WHERE contact_id(+) = su.ship_to_contact_id)
          SHIP_TO_CONTACT,

 CASE first_name WHEN NULL THEN NULL ELSE ', '||first_name END
 
Could be re-written as:

(SELECT last_name || DECODE (first_name, NULL, NULL, ', ' || first_name)
   FROM (SELECT ACCT_ROLE.CUST_ACCOUNT_ROLE_ID           AS CONTACT_ID,
       SUBSTRB (PARTY.PERSON_LAST_NAME, 1, 50)  AS last_name,
       SUBSTRB (PARTY.PERSON_FIRST_NAME, 1, 40) AS first_name
  FROM ar.HZ_CUST_ACCOUNT_ROLES ACCT_ROLE,
       ar.HZ_PARTIES            PARTY,
       ar.HZ_RELATIONSHIPS      REL,
       ar.HZ_ORG_CONTACTS       ORG_CONT,
       ar.HZ_PARTIES            REL_PARTY,
       ar.HZ_CUST_ACCOUNTS      ROLE_ACCT
WHERE     ACCT_ROLE.PARTY_ID = REL.PARTY_ID
       AND ACCT_ROLE.ROLE_TYPE = 'CONTACT'
       AND ORG_CONT.PARTY_RELATIONSHIP_ID = REL.RELATIONSHIP_ID
       AND REL.SUBJECT_ID = PARTY.PARTY_ID
       AND REL.PARTY_ID = REL_PARTY.PARTY_ID
       AND REL.SUBJECT_TABLE_NAME = 'HZ_PARTIES'
       AND REL.OBJECT_TABLE_NAME = 'HZ_PARTIES'
       AND ACCT_ROLE.CUST_ACCOUNT_ID = ROLE_ACCT.CUST_ACCOUNT_ID
       AND ROLE_ACCT.PARTY_ID = REL.OBJECT_ID)
  WHERE contact_id(+) = su.ship_to_contact_id)
          SHIP_TO_CONTACT,
