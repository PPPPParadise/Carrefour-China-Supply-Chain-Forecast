--  ======================================================
--                          Module 3
--                        BP dataflow
--      Step 2.2: Compute percentile IN promotion (HIVE)
-- =========================================================

-- Input: {database}.forecast_trxn_v7_full_item_id_sprint4    
-- Output: {database}.art_filter_promo
-- Usage: compute the 90th percentile for each sub-family IN promotion
-- Note: has to be run in Hive because of percentile_approx() function
--       and no need to rerun each time

create table {database}.art_filter_promo as
with promo_trxn_smart_group as
-- group by item_store_hour
(select
    item_id,
    sub_id,
    sub_family_code,
    store_code,
    substring(trxn_time,1,13) as trxn_time,
    sum(sales_qty) as sales_qty_sum
from {database}.forecast_trxn_v7_full_item_id_sprint4
where (current_dm_theme_id is not null
    or mpsp_disc > 0
    or promo_disc > 0
    or coupon_disc > 0)
    and sales_qty > 0
group by item_id, sub_id, trxn_time, store_code, sub_family_code),

promo_percentile as
-- promo trxn
(select
    sub_family_code,
    round(percentile_approx(sales_qty_sum, 0.90),0) as p90
from promo_trxn_smart_group
group by sub_family_code),

art_flag_added as
(select
    sub_family_code,
    p90
from promo_percentile),

safety_net as
(select
    sub_family_code,
    case when p90 < 50 then p90 else 50 end as p90
from art_flag_added)

select * from safety_net

