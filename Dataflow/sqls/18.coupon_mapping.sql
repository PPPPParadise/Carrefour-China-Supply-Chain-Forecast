/*
Description: Create table "temp.coupon_mapping"
             Add item_id to coupon_activity tables and make the linkages
Input:  temp.forecast_assortment_full
        nsa.coupon_activity_his
        nsa.coupon_activity_dtl_his
        nsa.coupon_dtl_setdate_his
        ods.nsa_coupon_item_detail_his

Output: temp.coupon_mapping 
*/

-- drop table if exists temp.coupon_mapping;

create table temp.coupon_mapping as
-- Get the item_id and item_code mapping from assortment
with item_code_id_mapping as (
select
    concat(dept_code,item_code) as item_code_8,
    sub_code,
    item_id,
    sub_id
from temp.forecast_assortment_full
where item_id is not null
group by concat(dept_code,item_code),sub_code, item_id, sub_id
),

-- tb1 coupon_activity_his
-- coupon_activity_id, storecitylevel, coupon_activity_type_id, coupon_typecode, coupon_count1
tb1_his as (
SELECT *
FROM nsa.coupon_activity_his
where status = '10'  -- the coupon activity has been implemented
),

-- tb2 coupon_activity_dtl_his
-- coupon_activity_d_id, coupon_city_store, promotion_level
tb2_his as (
SELECT *
FROM nsa.coupon_activity_dtl_his
),

-- tb3 coupon_dtl_setdate_his
-- coupon_activity_d_id, coupon_used_date_start, coupon_used_date_end
tb3_his as (
select
    coupon_activity_d_id,
    coupon_used_date_start,
    coupon_used_date_end
from nsa.coupon_dtl_setdate_his
),

-- tb4 nsa_coupon_item_detail_his
-- activity_id  activity_d_id and item_code, sub_code
tb4_his as (
select
    *
from ods.nsa_coupon_item_detail_his
),

tb5_his as (
select
    a.coupon_activity_id,
    a.coupon_activity_name_chn,
    a.coupon_activity_type_id,
    a.storecitylevel,
    a.bp_flag,
    a.coupon_city_store,
    a.coupon_typecode,
    a.coupon_count1,

    b.coupon_activity_d_id,
    b.coupon_area,
    b.coupon_city_store as coupon_city_store_dtl,
    b.promotion_level,

    c.coupon_used_date_start,
    c.coupon_used_date_end,

    d.item_code,
    d.sub_code

from tb1_his a

left join tb2_his b
on b.coupon_activity_id = a.coupon_activity_id

left join tb3_his c
on b.coupon_activity_d_id = c.coupon_activity_d_id

left join tb4_his d
on d.coupon_activity_d_id = cast(b.coupon_activity_d_id as string)
and d.coupon_activity_id = cast(a.coupon_activity_id as string)
),

-- tb1 coupon_activity
-- coupon_activity_id, storecitylevel, coupon_activity_type_id, coupon_typecode, coupon_count1
tb1 as (
SELECT *
FROM nsa.coupon_activity
where status = '10'  -- the coupon activity is ready to be implemented
),

-- tb2 coupon_activity_dtl
-- coupon_activity_d_id, coupon_city_store, promotion_level
tb2 as (
SELECT *
FROM nsa.coupon_activity_dtl
),

-- tb3 coupon_dtl_setdate
-- coupon_activity_d_id, coupon_used_date_start, coupon_used_date_end
tb3 as (
select
    coupon_activity_d_id,
    coupon_used_date_start,
    coupon_used_date_end 
from nsa.coupon_activity_dtl_setdate 
),

-- tb4 nsa_coupon_item_detail
-- activity_id  activity_d_id and item_code, sub_code
tb4 as (
select
    *
from ods.nsa_coupon_item_detail
),

tb5 as (
select
    a.coupon_activity_id,
    a.coupon_activity_name_chn,
    a.coupon_activity_type_id,
    a.storecitylevel,
    a.bp_flag,
    a.coupon_city_store,
    a.coupon_typecode,
    a.coupon_count1,

    b.coupon_activity_d_id,
    b.coupon_area,
    b.coupon_city_store as coupon_city_store_dtl,
    b.promotion_level,

    c.coupon_used_date_start,
    c.coupon_used_date_end,

    d.item_code,
    d.sub_code

from tb1 a

left join tb2 b
on b.coupon_activity_id = a.coupon_activity_id

left join tb3 c
on b.coupon_activity_d_id = c.coupon_activity_d_id

left join tb4 d
on d.coupon_activity_d_id = cast(b.coupon_activity_d_id as string)
and d.coupon_activity_id = cast(a.coupon_activity_id as string)
),

tb_union as (
select *
from tb5_his
union 
select *
from tb5 
),

add_item_id as (
select
    a.*,
    b.item_id,
    b.sub_id

from tb_union a
left join item_code_id_mapping b
on b.item_code_8 = a.item_code
and b.sub_code = a.sub_code
)

select *
from add_item_id
where item_id is not null
;

-- INVALIDATE METADATA temp.coupon_mapping;
