/* ==========================================================
                          Module 3
                         BP dataflow
    Step 3: Mapping transactions + Applying abnormal flag
=============================================================
*/

-- Input:{database}.forecast_trxn_v7_full_item_id_sprint4
--       {database}.forecast_trxn_v7_full_item_id_sprint4_group_id
--       {database}.art_filter_non_promo
--       {database}.art_filter_promo
--       ods.data8_media

-- Output: {database}.grouped_to_be_shipment
-- Usage: create a transaction table with a flag where baskets can be flag
--        by SCALA script based on abnormal flag and check information

create table {database}.grouped_to_be_shipment as
with group_id_info as (
-- adding groupid for each trxn
select
    a.*,
    b.group_id
from {database}.forecast_trxn_v7_full_item_id_sprint4 as a
left join ( select
                item_id,
                sub_id,
                store_code,
                ticket_id,
                max(group_id) as group_id
            from {database}.forecast_trxn_v7_full_item_id_sprint4_group_id_new
            group by item_id, sub_id, store_code, ticket_id
        ) as b
on a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and a.store_code = b.store_code
    and a.ticket_id = b.ticket_id
),

group_level as (
-- grouped according to groupid and date
select
    item_id,
    sub_id,
    store_code,
    max(sub_family_code) as sub_family_code,
    substring(cast(trxn_date as string),1,10) as trxn_date,
    max(ticket_id) as ticket_id,
    group_id,
    max(current_dm_theme_id) as current_dm_theme_id,
    sum(mpsp_disc) as mpsp_disc,
    sum(promo_disc) as promo_disc,
    sum(coupon_disc) as coupon_disc,
    sum(sales_qty) as sales_qty
from group_id_info
where sales_qty > 0
group by item_id, sub_id, store_code, trxn_date, group_id
),

adding_p90_non_promo as (
select
    a.*,
    b.p90
from (
    select *
    from group_level
    where current_dm_theme_id is null
        and mpsp_disc = 0
        and promo_disc = 0
        and coupon_disc = 0
    ) as a
left join {database}.art_filter_non_promo as b
on a.item_id = b.item_id
and a.sub_id = b.sub_id),

flag_adding_non_promo as (
    select
        *,
        case when (sales_qty > 175 * log10(p90 + 1) and group_id is not null) then 1 else 0 end as art_flag
    from adding_p90_non_promo),

adding_p90_promo as (
select
    a.*,
    b.p90
from (select
        *
    from group_level
    where current_dm_theme_id is not null
        or mpsp_disc > 0
        or promo_disc > 0
        or coupon_disc > 0) as a
left join {database}.art_filter_promo as b
on a.sub_family_code = b.sub_family_code),

flag_adding_promo as (
select
    *,
    case when (sales_qty > 200 * log10(p90 + 1) and group_id is not null) then 1 else 0 end as art_flag
from adding_p90_promo),

both_concat as (
select
    *
from flag_adding_non_promo
union all
select
    *
from flag_adding_promo),

-- zp_cp_flag as 
-- -- creating a zp_or_cp flag
-- (select
--     ticket_id,
--     item_id,
--     sub_id,
--     store_code,
--     1 as zp_or_cp
-- from ods.lddb_bp_cp_zp_final
-- where zp = 'Y'
--     or cp = 'Y'
-- group by ticket_id, item_id, sub_id, store_code
-- ),

check_pay_flag as (
-- creating a zp_or_cp flag
select
    ticket_id,
    1 as check_pay

from ods.data8_media 
where media_type = 2 
group by ticket_id 
),

combine_percentile_check_pay as (
-- adding flag to groups to combine with art_flag
select
    a.*,
    b.check_pay
from both_concat as a
left join check_pay_flag as b
on a.ticket_id = b.ticket_id
)

select
    item_id,
    sub_id,
    store_code,
    sub_family_code,
    trxn_date,
    ticket_id,
    group_id,
    current_dm_theme_id,
    mpsp_disc,
    promo_disc,
    coupon_disc,
    sales_qty,
    p90,
    case when (art_flag = 1 or check_pay = 1 or sales_qty >= 250) then 1 else 0 end as art_flag
from combine_percentile_check_pay
;

