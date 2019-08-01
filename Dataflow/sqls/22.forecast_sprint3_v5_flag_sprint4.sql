/*
Description: Create table "{database}.forecast_sprint3_v5_flag"
             Add coupon activity information to the dataset

Input:  {database}.coupon_city_store_union_flag_sprint4 
        {database}.forecast_sprint3_v3_flag_sprint4 
Output: {database}.forecast_sprint3_v5_flag_sprint4
*/

-- drop table if exists {database}.forecast_sprint3_v5_flag_sprint4 ;

create table {database}.forecast_sprint3_v5_flag_sprint4 as
-- 8899631 row(s)
with group_concat_info as (
    -- Group concat typeid and create feature "coupon_activity_type_id_full"
    with group_concat_typeid as (
        with tb1 as (
            SELECT
                item_id,
                sub_id,
                store_code,
                week_key,
                coupon_activity_type_id

            From {database}.coupon_city_store_union_flag_sprint4
            GROUP BY
                item_id,
                sub_id,
                store_code,
                week_key,
                coupon_activity_type_id
            order by coupon_activity_type_id desc
        )
        select
            item_id,
            sub_id,
            store_code,
            week_key,
            group_concat(coupon_activity_type_id) as coupon_activity_type_id_full
        from tb1
        group by
            item_id,
            sub_id,
            store_code,
            week_key
    ),
-- Group concat typecode and create feature "coupon_typecode_full"
group_concat_typecode as (
    with tb2 as (
        SELECT
            item_id,
            sub_id,
            store_code,
            week_key,
            coupon_typecode

        From {database}.coupon_city_store_union_flag_sprint4
        GROUP BY
            item_id,
            sub_id,
            store_code,
            week_key,
            coupon_typecode
        order by coupon_typecode desc
    )
    select
        item_id,
        sub_id,
        store_code,
        week_key,
        group_concat(coupon_typecode) as coupon_typecode_full

    from tb2
    group by
        item_id,
        sub_id,
        store_code,
        week_key
),
-- Group concat typecode and create feature "coupon_count1_full"
group_concat_count1 as (
    with tb3 as (
        SELECT
            item_id,
            sub_id,
            store_code,
            week_key,
            coupon_count1

        From {database}.coupon_city_store_union_flag_sprint4
        GROUP BY
            item_id,
            sub_id,
            store_code,
            week_key,
            coupon_count1
    )
    select
        item_id,
        sub_id,
        store_code,
        week_key,
        group_concat(coupon_count1) as coupon_count1_full

    from tb3
    group by
        item_id,
        sub_id,
        store_code,
        week_key
)

select
    a.item_id,
    a.sub_id,
    a.store_code,
    a.week_key,
    a.coupon_activity_type_id_full,
    b.coupon_typecode_full,
    c.coupon_count1_full

from group_concat_typeid a

left join group_concat_typecode b
on b.item_id = a.item_id
and b.sub_id = a.sub_id
and b.store_code = a.store_code
and b.week_key = a.week_key

left join group_concat_count1 c
on c.item_id = a.item_id
and c.sub_id = a.sub_id
and c.store_code = a.store_code
and c.week_key = a.week_key

),
-- Create coupon indicators
ndv_type_id_typecode as (
SELECT
    item_id,
    sub_id,
    store_code,
    week_key,
    -- number of distinct coupon_activity_type_id of one sub-item in one store during one week
    count(DISTINCT coupon_activity_type_id) as ndv_coupon_activity_type_id,
    -- number of distinct coupon_typecode of one sub-item in one store during one week
    count(DISTINCT coupon_typecode) as ndv_coupon_typecode,
    -- number of distinct coupon_count1 of one sub-item in one store during one week
    count(DISTINCT coupon_count1 ) as ndv_coupon_count1

From {database}.coupon_city_store_union_flag_sprint4
GROUP BY
    item_id,
    sub_id,
    store_code,
    week_key
),

add_coupon_type as (
select
    a.*,
    b.coupon_activity_type_id_full,
    b.coupon_typecode_full,
     b.coupon_count1_full,
    c.ndv_coupon_activity_type_id,
    c.ndv_coupon_typecode,
    c.ndv_coupon_count1

from {database}.forecast_sprint3_v3_flag_sprint4 a

left join group_concat_info b
on b.item_id = a.item_id
and b.sub_id = a.sub_id
and b.store_code = a.store_code
and b.week_key = a.week_key

left join ndv_type_id_typecode c
on c.item_id = a.item_id
and c.sub_id = a.sub_id
and c.store_code = a.store_code
and c.week_key = a.week_key
)

select
    *,
    -- Create dummy variables for different coupon type_ids
    if(instr(coupon_activity_type_id_full,'01') >= 1, 1, 0 ) as ind_coupon_typeid_DD,
    if(instr(coupon_activity_type_id_full,'02') >= 1, 1, 0 ) as ind_coupon_typeid_NP,
    if(instr(coupon_activity_type_id_full,'03') >= 1, 1, 0 ) as ind_coupon_typeid_AC,
    if(instr(coupon_activity_type_id_full,'04') >= 1, 1, 0 ) as ind_coupon_typeid_CC,

    -- Create dummy variables for different coupon type_code, take "CP", "MPM", "MP" separately (frequently appear)
    if(instr(coupon_typecode_full,'CP') >= 1, 1, 0)  as ind_coupon_typecode_CP,
    if(instr(coupon_typecode_full,'MPM') >= 1, 1, 0)  as ind_coupon_typecode_MPM,
    if(instr(coupon_typecode_full,'MP') >= 1, 1, 0)  as ind_coupon_typecode_MP,
    if((instr(coupon_typecode_full, 'CP') = 0
        and instr(coupon_typecode_full, 'MPM') = 0
        and instr(coupon_typecode_full, 'MP') = 0 ), 1, 0
        ) as ind_coupon_typecode_other,

    -- Create dummy variables for different coupon count1 mechanisms
    if(instr(coupon_count1_full,'27') >= 1, 1, 0 ) as ind_coupon_count1_27,
    if(instr(coupon_count1_full,'02|25') >= 1, 1, 0 ) as ind_coupon_count1_02_25,
    if(instr(coupon_count1_full,'60') >= 1, 1, 0 ) as ind_coupon_count1_60,
    if(instr(coupon_count1_full,'4') >= 1, 1, 0 ) as ind_coupon_count1_4,
    if(instr(coupon_count1_full,'1') >= 1, 1, 0 ) as ind_coupon_count1_1,
    if(instr(coupon_count1_full,'58') >= 1, 1, 0 ) as ind_coupon_count1_58,
    if(instr(coupon_count1_full,'11') >= 1, 1, 0 ) as ind_coupon_count1_11,
    if(instr(coupon_count1_full,'16|34') >= 1, 1, 0 ) as ind_coupon_count1_16_34,
    if(instr(coupon_count1_full,'56') >= 1, 1, 0 ) as ind_coupon_count1_56,
    if(instr(coupon_count1_full,'37') >= 1, 1, 0 ) as ind_coupon_count1_37,
    if(instr(coupon_count1_full,'44') >= 1, 1, 0 ) as ind_coupon_count1_44,
    if(instr(coupon_count1_full,'9') >= 1, 1, 0 ) as ind_coupon_count1_9,
    if(instr(coupon_count1_full,'10') >= 1, 1, 0 ) as ind_coupon_count1_10,
    if((instr(coupon_count1_full, '27') = 0
        and instr(coupon_count1_full, '02|25') = 0
        and instr(coupon_count1_full, '60') = 0
        and instr(coupon_count1_full, '4') = 0
        and instr(coupon_count1_full, '1') = 0
        and instr(coupon_count1_full, '58') = 0
        and instr(coupon_count1_full, '11') = 0
        and instr(coupon_count1_full, '16|34') = 0
        and instr(coupon_count1_full, '56') = 0
        and instr(coupon_count1_full, '37') = 0
        and instr(coupon_count1_full, '44') = 0
        and instr(coupon_count1_full, '9') = 0
        and instr(coupon_count1_full, '10') = 0
        ), 1, 0
        ) as ind_coupon_count1_other

from add_coupon_type
;

-- INVALIDATE METADATA {database}.forecast_sprint3_v5_flag_sprint4;
