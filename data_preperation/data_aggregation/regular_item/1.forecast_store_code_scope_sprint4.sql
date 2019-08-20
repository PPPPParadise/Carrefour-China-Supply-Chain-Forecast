/*
Description: Create table "{database}.forecast_store_code_scope_sprint4",
             listing all the store_code in our scope

Input:  ods.p4md_stogld
        ods.p4md_stoatt

Output: {database}.forecast_store_code_scope_sprint4
*/

-- drop table if exists {database}.forecast_store_code_scope_sprint4;
create table {database}.forecast_store_code_scope_sprint4 as
with bad_storecode as (
    SELECT
        stastocd
    FROM ods.p4md_stoatt
    WHERE stacd = 'CLO'
    AND staclcd ='STCD'
    AND stasdate < current_timestamp()
    AND staedate >= current_timestamp()
),
east_store_code as (
    SELECT
        stostocd
    FROM ods.p4md_stogld
    WHERE stostate = 'T3'
    AND stoformat <> 'CVS'
),
combine as (
    select
        a.*,
        b.*
    from east_store_code a
    left join bad_storecode b
    on b.stastocd = a.stostocd
)
select
    stostocd
from combine
where stastocd is null
and stostocd <> '812'
;

-- INVALIDATE METADATA  {database}.forecast_store_code_scope_sprint4;
