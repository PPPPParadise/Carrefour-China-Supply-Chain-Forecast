/* =======================================================
                           Module 3
                        Service level
     Step 1: Compute service level per item-supplier
==========================================================
*/

-- From: lfms.daily_dctrxn
-- To: vartefact.service_level_safety2
--     vartefact.service_level_calculation2 (intermediate table)
-- Usage: compute the service level for each item-supplier-date
--        then take the closest date available for each item-supplier
-- Note: _
--     Author: Baudouin              2019-07
--     with revisions from Qiang     2019-08
--     with optimizations from Vinc  2019-08

create table vartefact.service_level_calculation2 as

with combine_ordered_received as
-- combine each DC trxn with the received order
(
    select
        a.item_id,
        a.dept_code,
        a.item_code,
        a.sub_code,
        -- a.supplier_code,
        a.date_key,
        -- max(a.dept_code) as dept_code,
        sum(a.trxn_qty) as trxn_qty,
        sum(b.basic_order_qty) as basic_order_qty
    from lfms.daily_dctrxn a
        inner join lfms.ord b
        on
            a.dept_code = b.department_code
            and a.item_code = b.item_code
            and a.sub_code = b.sub_code
            and a.order_number = b.order_number
            -- and a.supplier_code = b.supplier_code
    where
        a.item_id in (
            select item_id
            from vartefact.forecast_itemid_list_threebrands_20190424)
        and a.dc_site = 'DC1'
        and b.order_status <> 'L'   -- cancelled orders
        and b.mother_warehouse_code not in ('KP01', 'kp01') -- excluded BPs
        -- and b.mother_warehouse_code = 'KS01'
        and a.date_key >= '20180101'
    group by a.item_id, a.dept_code, a.item_code, a.sub_code, a.date_key
),

ordered_trxn as
-- order them in time
(
    select
        *,
        row_number() over (partition by item_code, sub_code
                           order by date_key) as rownum
    from combine_ordered_received
),

last_8_trxns as
-- pick last 8 trxns
(
    select
        a.item_id,
        a.dept_code,
        a.item_code,
        a.sub_code,
        -- a.supplier_code,
        a.rownum,
        a.date_key,
        sum(b.trxn_qty) as trxn_qty_last8,
        sum(b.basic_order_qty) as basic_order_qty_last8,
        count(*) as last_trxns_count     -- check if we can get 8 trxns or not
    from
        ordered_trxn a
        inner join ordered_trxn b
        on
            a.dept_code = b.dept_code
            and a.item_code = b.item_code
            and a.sub_code = b.sub_code
            -- and a.supplier_code = b.supplier_code
            and a.rownum between b.rownum and b.rownum + 7
    group by a.item_id, a.dept_code, a.item_code, a.sub_code, a.rownum, a.date_key
),

last_month_trxns as
-- last year trxns
(
    select
        a.item_id,
        a.dept_code,
        a.item_code,
        a.sub_code,
        -- a.supplier_code,
        a.rownum,
        a.date_key,
        a.trxn_qty,
        a.basic_order_qty,
        sum(b.trxn_qty) as trxn_qty_lastmonth,
        sum(b.basic_order_qty) as basic_order_qty_lastmonth,
        count(*) as compte
    from ordered_trxn a
        inner join ordered_trxn b
        on a.item_code = b.item_code
            and a.sub_code = b.sub_code
            -- and a.supplier_code = b.supplier_code
            and to_date(to_timestamp(a.date_key,'yyyyMMdd'))
                between to_date(to_timestamp(b.date_key,'yyyyMMdd'))
                and add_months(to_date(to_timestamp(b.date_key,'yyyyMMdd')), 1)
    group by
        a.item_id, a.dept_code, a.item_code, a.sub_code, a.rownum, a.date_key,
        a.trxn_qty, a.basic_order_qty
),

last_month_or_8_trxns as
-- pick in priority last month (if count > 8) for each DC_order_trxn
(select
    a.*,
    b.trxn_qty_last8,
    b.basic_order_qty_last8,
    b.last_trxns_count,
    c.trxn_qty_lastmonth,
    c.basic_order_qty_lastmonth,
    c.compte,
    case when c.compte >= 8 then c.trxn_qty_lastmonth / c.basic_order_qty_lastmonth
    else b.trxn_qty_last8 / b.basic_order_qty_last8 end as service_level
from ordered_trxn a
    inner join last_8_trxns b
        on
            a.dept_code = b.dept_code
            and a.item_code = b.item_code
            and a.sub_code = b.sub_code
            -- and a.supplier_code = b.supplier_code
            and a.rownum = b.rownum
    inner join last_month_trxns c
        on a.item_code = c.item_code
            and a.sub_code = c.sub_code
            -- and a.supplier_code = c.supplier_code
            and a.rownum = c.rownum),

order_trxn_ordered as
(select
    *,
    row_number() over(partition by dept_code, item_code, sub_code
                      order by date_key desc) as rownum2
from last_month_or_8_trxns)

select *
from order_trxn_ordered
where rownum2 = 1
                               
                               