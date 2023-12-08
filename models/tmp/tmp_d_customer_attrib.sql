SELECT 
    SRC.* 
FROM {{ref('stg_customer')}} SRC
LEFT JOIN (SELECT *
            FROM DBT_POC_DB.DBT_POC_TGT.EDW_D_CUSTOMER_ATTRIB
            WHERE RCD_OPEN_FLG = 'Y'
        ) TGT
ON TGT.CUS_ID = SRC.CUS_ID
WHERE 1 = 0
    OR SRC.age_qualifier_cde <> TGT.age_qualifier_cde
    OR SRC.city <> TGT.city 
    OR SRC.state_cde <> TGT.state_cde 
    OR SRC.country_cde <> TGT.country_cde 
    OR SRC.cus_ethnicity_cde <> TGT.cus_ethnicity_cde
    OR SRC.loyalty_tier <> TGT.loyalty_tier 
    OR SRC.net_spend_l24 <> TGT.net_spend_l24 
    OR SRC.estimated_income <> TGT.estimated_income 
    OR SRC.first_txn_dt <> TGT.first_txn_dt 
    OR SRC.last_txn_dt <> TGT.last_txn_dt
    OR TGT.CUS_ID IS NULL