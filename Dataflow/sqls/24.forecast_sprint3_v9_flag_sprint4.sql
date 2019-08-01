/*
Description: Create table "{database}.forecast_sprint3_v9_flag"
             Add last years related sales information
Input:  {database}.forecast_sprint3_v6_flag_sprint4 
        ods.dim_calendar 
Output: {database}.forecast_sprint3_v9_flag_sprint4
*/

-- ============== LAST_YEAR_SALES_QTY (correction) ==============
-- drop table if exists {database}.forecast_sprint3_v9_flag_sprint4;

create table {database}.forecast_sprint3_v9_flag_sprint4 as

-- 8898213 row(s)
-- adding last_year_sales_qty_new feature
with last_year_week_added as
-- adding last year week_key
(select
    a.*,
    b.last_year_week_key,
    b.last_year_week_key_m1bef,
    b.last_year_week_key_m1aft
from {database}.forecast_sprint3_v6_flag_sprint4 as a
left join
(select
    week_key,
    concat(cast(year(date_sub(min(date_value), 364)) as string),lpad(cast(weekofyear(date_sub(min(date_value), 364)) as string),2,'0')) as last_year_week_key,
    concat(cast(year(date_sub(min(date_value), 395)) as string),lpad(cast(weekofyear(date_sub(min(date_value), 395)) as string),2,'0')) as last_year_week_key_m1bef,
    concat(cast(year(date_sub(min(date_value), 333)) as string),lpad(cast(weekofyear(date_sub(min(date_value), 333)) as string),2,'0')) as last_year_week_key_m1aft
from ods.dim_calendar 
group by week_key
) as b
on a.week_key = b.week_key
),

last_year_sales_qty_all_before as
-- adding every sales_qty before last year week_key - 1 month
(select
    a.*,
    b.week_key as week_key_before,
    b.sales_qty_sum as last_year_sales_qty_bef
from last_year_week_added as a
left join
(select
    item_id,
    sub_id,
    store_code,
    week_key,
    sales_qty_sum
from last_year_week_added
) as b
on a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and a.store_code = b.store_code
    and a.last_year_week_key >= b.week_key
    and a.last_year_week_key_m1bef <= b.week_key
  ),

last_year_duplicates_plus_rownum as
(select
    *,
    row_number() over(partition by store_code,item_id,sub_id,week_key order by week_key_before desc) as rownum
from last_year_sales_qty_all_before
),

last_year_no_duplicates as
-- drop duplicates
(select
    *
from last_year_duplicates_plus_rownum
where rownum = 1
),

last_year_sales_qty_all_after as
-- adding every sales_qty after last year week_key + 1 month
(select
    a.*,
    b.week_key as week_key_after,
    b.sales_qty_sum as last_year_sales_qty_aft
from last_year_no_duplicates as a
left join
(select
    item_id,
    sub_id,
    store_code,
    week_key,
    sales_qty_sum
from last_year_no_duplicates) as b
on a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and a.store_code = b.store_code
    and a.last_year_week_key <= b.week_key
    and a.last_year_week_key_m1aft >= b.week_key),

last_year_duplicates_plus_rownum_bis as
(select
    *,
    row_number() over(partition by store_code,item_id,sub_id,week_key order by week_key_after asc) as rownum_bis
from last_year_sales_qty_all_after),

last_year_no_duplicates_bis as
-- drop duplicates
(select
    *
from last_year_duplicates_plus_rownum_bis
where rownum_bis = 1),

combining_both as
(select
    *,
    case when last_year_sales_qty_bef is not null then last_year_sales_qty_bef
         else last_year_sales_qty_aft end as last_year_sales_qty_new

from last_year_no_duplicates_bis
)

select
    *
from combining_both
;
-- 1920596 / 4168024 => 46.1% in total scope
-- 1826192 / 2356750 => 77.4% in year >= 2018 scope

-- INVALIDATE METADATA {database}.forecast_sprint3_v9_flag_sprint4;
