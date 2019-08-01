--  =========================================================
--                           Module 3
--                         BP dataflow
--     Step 2.1: Compute percentile OUTSIDE promotion (HIVE)
-- ============================================================


-- Input: {database}.forecast_trxn_v7_full_item_id_sprint4
-- Output: {database}.art_filter_non_promo 
-- Usage: compute the 90th percentile for each item OUTSIDE promotion
-- Note: has to be run in Hive because of percentile_approx() function
--       and no need to rerun each time

create table {database}.art_filter_non_promo as
with non_promo_trxn_smart_group as
-- group by item_store_hour
(select
    item_id,
    sub_id,
    store_code,
    substring(trxn_time,1,13) as trxn_time,
    sum(sales_qty) as sales_qty_sum
from {database}.forecast_trxn_v7_full_item_id_sprint4
where current_dm_theme_id is null
    and mpsp_disc = 0
    and promo_disc = 0
    and coupon_disc = 0
    and sales_qty > 0
group by item_id, sub_id, trxn_time, store_code),

non_promo_percentile as
-- non_promo trxn
(select
    item_id,
    sub_id,
    round(percentile_approx(sales_qty_sum, 0.90),0) as p90
from non_promo_trxn_smart_group
group by item_id, sub_id),

art_flag_added as
(select
    item_id,
    sub_id,
    p90
from non_promo_percentile),

safety_net as
(select
    item_id,
    sub_id,
    case when p90 < 50 then p90 else 50 end as p90
from art_flag_added)

select * from safety_net
