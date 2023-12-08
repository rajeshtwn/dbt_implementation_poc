{{
     config(
         materialized='incremental',
         pre_hook = """
            update dbt_poc_db.dbt_poc_tgt.tgt_inv_ild_b tgt
            set     tgt.end_day_key = src.day_key -1,
                    tgt.rcd_upd_ts = current_timestamp()
            FROM dbt_poc_db.dbt_poc_tmp.tmp_INV_ILD_B src
            WHERE 1 = 1
                AND src.ITEM_KEY = tgt.ITEM_KEY 
                AND src.LOCATION_KEY = tgt.LOCATION_KEY                       
                AND tgt.DAY_KEY < src.DAY_KEY 
         """,
        post_hook = """
            ---- DELETE CHANGED RECORDS 
            DELETE FROM dbt_poc_db.dbt_poc_tgt.tgt_inv_il_b  tgt
            WHERE EXISTS (SELECT 1 FROM dbt_poc_db.dbt_poc_TMP.TMP_INV_ILD_B src
                            WHERE     src.ITEM_KEY = tgt.ITEM_KEY AND src.LOCATION_KEY = tgt.LOCATION_KEY 
                            AND       src.DAY_KEY = (select distinct day_key from dbt_poc_db.dbt_poc_tgt.dwh_d_curr_tim_lu)
                        ) ;

            ---- INSERT NEW RECORDS
            INSERT INTO dbt_poc_db.dbt_poc_tgt.tgt_inv_il_b (
                item_key
                ,location_key
                ,f_oh_qty
                ,f_oh_cst
                ,f_oh_rtl
                ,RCD_INS_TS
                ,RCD_UPD_TS
            )
            SELECT 
                item_key
                ,location_key
                ,f_oh_qty
                ,f_oh_cst
                ,f_oh_rtl
                ,CURRENT_TIMESTAMP RCD_INS_TS
                ,CURRENT_TIMESTAMP RCD_UPD_TS
            FROM dbt_poc_db.dbt_poc_TMP.TMP_INV_ILD_B SRC
            WHERE (ITEM_KEY, LOCATION_KEY) NOT IN (SELECT ITEM_KEY, LOCATION_KEY FROM dbt_poc_db.dbt_poc_tgt.tgt_inv_il_b)

        """
     )
}}
SELECT * FROM (
    SELECT
        SRC.ITEM_KEY        AS ITEM_KEY,
        SRC.LOCATION_KEY    AS LOCATION_KEY,
        SRC.DAY_KEY         AS DAY_KEY,
        '9999-12-31'        AS END_DAY_KEY,
        SRC.F_OH_QTY        AS F_OH_QTY,
        SRC.F_OH_CST        AS F_OH_CST,
        SRC.F_OH_RTL        AS F_OH_RTL,
        CURRENT_TIMESTAMP() AS RCD_INS_TS,  
        CURRENT_TIMESTAMP() AS RCD_UPD_TS
    FROM {{ref('tmp_inv_ild_b')}} SRC
    )

