/*
Description: Create table "{database}.forecast_sprint3_v6_flag"
             Add coupon activity information to the dataset

Input:  {database}.forecast_sprint3_v5_flag_sprint4
Output: {database}.forecast_sprint3_v6_flag
*/

-- drop table if exists {database}.forecast_sprint3_v6_flag_sprint4;

create table {database}.forecast_sprint3_v6_flag_sprint4 as
-- 8899631 row(s)
with sales_per_item_fam as (
    select
        a.*,
        b.fam_store_week_sales_qty / b.fam_item_count as fam_sales_item_ratio
    from {database}.forecast_sprint3_v5_flag_sprint4 as a
    left join (
            select
                family_code,
                store_code,
                week_key,
                sum(sales_qty_sum) as fam_store_week_sales_qty,
                count(*) as fam_item_count
            from {database}.forecast_sprint3_v5_flag_sprint4
            group by family_code, store_code, week_key
        ) as b
        on a.family_code = b.family_code
        and a.store_code = b.store_code
        and a.week_key = b.week_key
        ),

-- applying rolling on: family sales in average per item
fam_sales_item_ratio_roll as (
    select
        *,
        avg(fam_sales_item_ratio) over (partition by store_code,item_id,sub_id order by week_key asc rows between 4 preceding and 1 preceding) as fam_sales_item_ratio_4w,
        avg(fam_sales_item_ratio) over (partition by store_code,item_id,sub_id order by week_key asc rows between 12 preceding and 1 preceding) as fam_sales_item_ratio_12w,
        avg(fam_sales_item_ratio) over (partition by store_code,item_id,sub_id order by week_key asc rows between 24 preceding and 1 preceding) as fam_sales_item_ratio_24w,
        avg(fam_sales_item_ratio) over (partition by store_code,item_id,sub_id order by week_key asc rows between 52 preceding and 1 preceding) as fam_sales_item_ratio_52w
    from sales_per_item_fam
    ),

-- sales in average per item within the sub_family
sales_per_item_subfam as (
    select
        a.*,
        b.subfam_store_week_sales_qty / b.subfam_item_count as subfam_sales_item_ratio
    from fam_sales_item_ratio_roll as a
    left join (
        select
        sub_family_code,
        store_code,
        week_key,
        sum(sales_qty_sum) as subfam_store_week_sales_qty,
        count(*) as subfam_item_count
    from {database}.forecast_sprint3_v5_flag_sprint4
    group by sub_family_code, store_code, week_key
    ) as b
    on a.sub_family_code = b.sub_family_code
    and a.store_code = b.store_code
    and a.week_key = b.week_key),

-- applying rolling on: family sales in average per item
subfam_sales_item_ratio_roll as (
    select
        *,
        avg(subfam_sales_item_ratio) over (partition by store_code,item_id,sub_id order by week_key asc rows between 4 preceding and 1 preceding) as subfam_sales_item_ratio_4w,
        avg(subfam_sales_item_ratio) over (partition by store_code,item_id,sub_id order by week_key asc rows between 12 preceding and 1 preceding) as subfam_sales_item_ratio_12w,
        avg(subfam_sales_item_ratio) over (partition by store_code,item_id,sub_id order by week_key asc rows between 24 preceding and 1 preceding) as subfam_sales_item_ratio_24w,
        avg(subfam_sales_item_ratio) over (partition by store_code,item_id,sub_id order by week_key asc rows between 52 preceding and 1 preceding) as subfam_sales_item_ratio_52w
    from sales_per_item_subfam
)

select
    *
from subfam_sales_item_ratio_roll
;


-- INVALIDATE METADATA {database}.forecast_sprint3_v6_flag_sprint4;
