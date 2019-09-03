INSERT overwrite vartefact.forecast_item_code_id_stock (
    con_holding,
    date_key,
    store_code,
    dept_code,
    item_code,
    sub_code,
    item_id,
    sub_id,
    balance_qty,
    main_supplier,
    ds_supplier,
    stop_year,
    stop_month,
    stop_reason
)
SELECT
    DISTINCT id.con_holding,
    pds.date_key,
    pds.store_code,
    pds.dept_code,
    pds.item_code,
    pds.sub_code,
    pds.item_id,
    pds.sub_id,
    pds.balance_qty,
    pds.main_supplier,
    pds.ds_supplier,
    pds.stop_year,
    pds.stop_month,
    pds.stop_reason
FROM
    fds.p4cm_daily_stock pds
    JOIN (
        select
            sd.item_code,
            sd.sub_code,
            sd.dept_code,
            sd.con_holding
        from
            vartefact.forecast_store_item_details sd
        union
        select
            dc.item_code,
            dc.sub_code,
            dc.dept_code,
            dc.holding_code as con_holding
        from
            vartefact.forecast_dc_item_details dc
    ) id ON pds.item_code = id.item_code
    AND pds.sub_code = id.sub_code
    AND pds.dept_code = id.dept_code
WHERE
    pds.date_key = '{0}'