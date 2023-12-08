{% snapshot edw_d_customer_snapshot %}

{{ config(
  materialized='snapshot',
  target_database='dbt_poc_db', 
  target_schema='dbt_poc_tgt',      
  strategy='check',
  unique_key='cus_id',
  check_cols=['city','state_cde','country_cde','loyalty_tier','net_spend_l24'],
  invalidate_hard_deletes = True
) }}

select
    *
from
  {{ ref('stg_customer')}} -- Specify your source and source model

{% endsnapshot %}
