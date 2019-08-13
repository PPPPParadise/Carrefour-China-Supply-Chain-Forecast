insert overwrite vartefact.forecast_nsa_dm_extract_log partition (dm_theme_id, date_key)
select
    item_code,
    sub_code,
    city_code,
    extract_order,
    dept_code,
    npp,
    ppp,
    ppp_start_date,
    ppp_end_date,
    city_name,
    holding_code,
    item_id,
    sub_id,
    load_date,
    dm_theme_id,
    from_unixtime(unix_timestamp(load_date), 'yyyyMMdd') as date_key
from
    nsa.dm_extract_log
where
    load_date > to_timestamp('{0}', 'yyyy-MM-dd')
    and ppp_start_date >= '{0}'