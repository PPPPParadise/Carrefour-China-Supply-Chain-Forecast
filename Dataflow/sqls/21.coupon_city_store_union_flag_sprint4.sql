/*
Description: Create table "{database}.coupon_city_store_union"
             restructure {database}.forecast_sprint3_coupon_item_link_flag
             Differentiate city-level coupons and store-level coupons by "promotion_level_city"

Input:  {database}.forecast_sprint3_coupon_item_link_flag_sprint4 

Output: {database}.coupon_city_store_union_flag
*/

-- drop table if exists {database}.coupon_city_store_union_flag_sprint4;

create table {database}.coupon_city_store_union_flag_sprint4 as
-- 6174661 row(s)
with city_coupon as (
SELECT
    item_id,
    sub_id,
    city_code,
    store_code,
    week_key,
    coupon_activity_id_city as coupon_activity_id,
    coupon_activity_d_id_city as coupon_activity_d_id,
    coupon_activity_type_id_city as coupon_activity_type_id,
    coupon_typecode_city as coupon_typecode,
    coupon_count1_city as coupon_count1,
    promotion_level_city as promotion_level_city

from {database}.forecast_sprint3_coupon_item_link_flag_sprint4
-- only select coupon activities that are planned on city level
where coupon_activity_id_city is not null
),

store_coupon as (
SELECT
    item_id,
    sub_id,
    city_code,
    store_code,
    week_key,
    coupon_activity_id_store as coupon_activity_id,
    coupon_activity_d_id_store as coupon_activity_d_id,
    coupon_activity_type_id_store as coupon_activity_type_id,
    coupon_typecode_store as coupon_typecode,
    coupon_count1_store as coupon_count1,
    promotion_level_store as promotion_level_city

from {database}.forecast_sprint3_coupon_item_link_flag_sprint4
-- only select coupon activities that are planned on store level
where coupon_activity_id_store is not null
)

select
    *
from (
    select
        *
    from city_coupon
    union
    select
        *
    from store_coupon
) table0
;

-- INVALIDATE METADATA {database}.coupon_city_store_union_flag_sprint4;
