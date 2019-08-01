/* =====================================================
                         Module 3
                       DM dataflow
     Step 7: Compute average discount per coupon
========================================================
*/

-- From: {database}.forecast_sprint3_v10_flag_sprint4
--       ods.dim_calendar
--       {database}.forecast_trxn_planned_bp_excluded_v3
--       {database}.forecast_sprint4_promo_with_baseline
-- To: {database}.forecast_sprint4_promo_mecha_v4
-- Usage: adding the average discount available for the current coupon,
--        and if multiple counpons are avaiblable we take the one with the highest
--        average discount
-- Note: {database}.forecast_sprint3_v10_flag_sprint4 don't need
--       to be refreshed on a daily basis

create table {database}.forecast_sprint4_promo_with_coupon as
with only_one as
(select
    *,
    case when (ind_coupon_count1_27 + ind_coupon_count1_02_25 + ind_coupon_count1_60
        + ind_coupon_count1_4 + ind_coupon_count1_1 + ind_coupon_count1_58
        + ind_coupon_count1_11 + ind_coupon_count1_16_34 + ind_coupon_count1_56
        + ind_coupon_count1_37 + ind_coupon_count1_44 + ind_coupon_count1_9
        + ind_coupon_count1_10 + ind_coupon_count1_other) = 1 then 1 else 0 end as one_coup,
    (coupon_disc_sum / nullif(ext_amt_sum,0)) as coup_disc_ratio
from {database}.forecast_sprint3_v10_flag_sprint4),

coupon_27 as
(select
    '27' as coupon,
    avg(coup_disc_ratio) as disc_ratio_avg,
    stddev_pop(coup_disc_ratio) as disc_ratio_std,
    count(1) as unique_count
from only_one
where (ind_coupon_count1_27 = 1) and (one_coup = 1)),

coupon_60 as
(select
    '60' as coupon,
    avg(coup_disc_ratio) as disc_ratio_avg,
    stddev_pop(coup_disc_ratio) as disc_ratio_std,
    count(1) as unique_count
from only_one
where (ind_coupon_count1_60 = 1) and (one_coup = 1)),

coupon_4 as
(select
    '4' as coupon,
    avg(coup_disc_ratio) as disc_ratio_avg,
    stddev_pop(coup_disc_ratio) as disc_ratio_std,
    count(1) as unique_count
from only_one
where (ind_coupon_count1_4 = 1) and (one_coup = 1)),

coupon_1 as
(select
    '1' as coupon,
    avg(coup_disc_ratio) as disc_ratio_avg,
    stddev_pop(coup_disc_ratio) as disc_ratio_std,
    count(1) as unique_count
from only_one
where (ind_coupon_count1_1 = 1) and (one_coup = 1)),

coupon_58 as
(select
    '58' as coupon,
    avg(coup_disc_ratio) as disc_ratio_avg,
    stddev_pop(coup_disc_ratio) as disc_ratio_std,
    count(1) as unique_count
from only_one
where (ind_coupon_count1_58 = 1) and (one_coup = 1)),

coupon_11 as
(select
    '11' as coupon,
    avg(coup_disc_ratio) as disc_ratio_avg,
    stddev_pop(coup_disc_ratio) as disc_ratio_std,
    count(1) as unique_count
from only_one
where (ind_coupon_count1_11 = 1) and (one_coup = 1)),

coupon_56 as
(select
    '56' as coupon,
    avg(coup_disc_ratio) as disc_ratio_avg,
    stddev_pop(coup_disc_ratio) as disc_ratio_std,
    count(1) as unique_count
from only_one
where (ind_coupon_count1_56 = 1) and (one_coup = 1)),

coupon_37 as
(select
    '37' as coupon,
    avg(coup_disc_ratio) as disc_ratio_avg,
    stddev_pop(coup_disc_ratio) as disc_ratio_std,
    count(1) as unique_count
from only_one
where (ind_coupon_count1_37 = 1) and (one_coup = 1)),

coupon_44 as
(select
    '44' as coupon,
    avg(coup_disc_ratio) as disc_ratio_avg,
    stddev_pop(coup_disc_ratio) as disc_ratio_std,
    count(1) as unique_count
from only_one
where (ind_coupon_count1_44 = 1) and (one_coup = 1)),

coupon_9 as
(select
    '9' as coupon,
    avg(coup_disc_ratio) as disc_ratio_avg,
    stddev_pop(coup_disc_ratio) as disc_ratio_std,
    count(1) as unique_count
from only_one
where (ind_coupon_count1_9 = 1) and (one_coup = 1)),

coupon_10 as
(select
    '10' as coupon,
    avg(coup_disc_ratio) as disc_ratio_avg,
    stddev_pop(coup_disc_ratio) as disc_ratio_std,
    count(1) as unique_count
from only_one
where (ind_coupon_count1_10 = 1) and (one_coup = 1)),

coupon_table as
(select * from coupon_27
union
select * from coupon_60
union
select * from coupon_4
union
select * from coupon_1
union
select * from coupon_58
union
select * from coupon_11
union
select * from coupon_56
union
select * from coupon_37
union
select * from coupon_44
union
select * from coupon_9
union
select * from coupon_10),

cp_27 as
(select
    a.*,
    b.disc_ratio_avg as disc_ratio_avg_27
from only_one as a
left join
coupon_27 as b
on a.ind_coupon_count1_27 = 1),

cp_60 as
(select
    a.*,
    b.disc_ratio_avg as disc_ratio_avg_60
from cp_27 as a
left join
coupon_60 as b
on a.ind_coupon_count1_60 = 1),

cp_4 as
(select
    a.*,
    b.disc_ratio_avg as disc_ratio_avg_4
from cp_60 as a
left join
coupon_4 as b
on a.ind_coupon_count1_4 = 1),

cp_1 as
(select
    a.*,
    b.disc_ratio_avg as disc_ratio_avg_1
from cp_4 as a
left join
coupon_1 as b
on a.ind_coupon_count1_1 = 1),

cp_58 as
(select
    a.*,
    b.disc_ratio_avg as disc_ratio_avg_58
from cp_1 as a
left join
coupon_58 as b
on a.ind_coupon_count1_58 = 1),

cp_11 as
(select
    a.*,
    b.disc_ratio_avg as disc_ratio_avg_11
from cp_58 as a
left join
coupon_11 as b
on a.ind_coupon_count1_11 = 1),

cp_56 as
(select
    a.*,
    b.disc_ratio_avg as disc_ratio_avg_56
from cp_11 as a
left join
coupon_56 as b
on a.ind_coupon_count1_56 = 1),

cp_37 as
(select
    a.*,
    b.disc_ratio_avg as disc_ratio_avg_37
from cp_56 as a
left join
coupon_37 as b
on a.ind_coupon_count1_37 = 1),

cp_44 as
(select
    a.*,
    b.disc_ratio_avg as disc_ratio_avg_44
from cp_37 as a
left join
coupon_44 as b
on a.ind_coupon_count1_44 = 1),

cp_9 as
(select
    a.*,
    b.disc_ratio_avg as disc_ratio_avg_9
from cp_44 as a
left join
coupon_9 as b
on a.ind_coupon_count1_9 = 1),

cp_10 as
(select
    a.*,
    b.disc_ratio_avg as disc_ratio_avg_10
from cp_9 as a
left join
coupon_10 as b
on a.ind_coupon_count1_10 = 1),

max_step1 as
(select
    *,
    case when ifnull(disc_ratio_avg_27,0) > ifnull(disc_ratio_avg_60,0) then ifnull(disc_ratio_avg_27,0) else ifnull(disc_ratio_avg_60,0) end as max_1,
    case when ifnull(disc_ratio_avg_4,0) > ifnull(disc_ratio_avg_1,0) then ifnull(disc_ratio_avg_4,0) else ifnull(disc_ratio_avg_1,0) end as max_2,
    case when ifnull(disc_ratio_avg_58,0) > ifnull(disc_ratio_avg_11,0) then ifnull(disc_ratio_avg_58,0) else ifnull(disc_ratio_avg_11,0) end as max_3,
    case when ifnull(disc_ratio_avg_56,0) > ifnull(disc_ratio_avg_37,0) then ifnull(disc_ratio_avg_56,0) else ifnull(disc_ratio_avg_37,0) end as max_4,
    case when ifnull(disc_ratio_avg_44,0) > ifnull(disc_ratio_avg_9,0) then ifnull(disc_ratio_avg_44,0) else ifnull(disc_ratio_avg_9,0) end as max_5,
    ifnull(disc_ratio_avg_10,0) as max_6
from cp_10),

max_step2 as
(select
    *,
    case when max_1 > max_2 then max_1 else max_2 end as b_1,
    case when max_3 > max_4 then max_3 else max_4 end as b_2,
    case when max_5 > max_6 then max_5 else max_6 end as b_3
from max_step1),

max_step3 as
(select
    *,
    case when b_1 >= b_2 and b_1 >= b_3 then b_1
         when b_2 >= b_1 and b_2 >= b_3 then b_2
         when b_3 >= b_1 and b_3 >= b_2 then b_3 else 0 end as coupon_disc_ratio_avg_max
from max_step2),

max_coupon_disc_to_add_to_big_table as
(select
    item_id,
    sub_id,
    store_code,
    week_key,
    coupon_disc_ratio_avg_max
from max_step3),

add_dupplicates as
-- adding coupon information to big table
(select
    a.*,
    b.coupon_disc_ratio_avg_max
from {database}.forecast_sprint4_promo_with_baseline as a
left join
max_coupon_disc_to_add_to_big_table as b
on a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and a.store_code = b.store_code
    and a.psp_end_week >= b.week_key
    and a.psp_start_week <= b.week_key),

remove_dupplicates_dm as
-- remove dupplicates
(select
    item_id,
    sub_id,
    store_code,
    current_dm_theme_id,
    sub_code,
    city_code,
    dm_sales_qty,
    out_of_stock_flag,
    nl,
    current_dm_theme_en_desc,
    current_theme_start_date,
    current_theme_end_date,
    current_dm_psp,
    current_dm_nsp,
    current_dm_psp_nsp_ratio,
    current_dm_psp_start_date,
    current_dm_psp_end_date,
    current_dm_page_strategy_code,
    current_dm_slot_type_code,
    current_dm_slot_type_name,
    current_dm_page_no,
    item_seasonal_code,
    sub_family_code,
    family_code,
    psp_start_week,
    psp_start_month,
    psp_end_week,
    last_year_dm_sales,
    last_year_dm_psp_nsp_ratio,
    last_year_fam_dm_sales_avg,
    last_5dm_sales_avg,
    fam_last_5dm_sales_avg,
    festival_type,
    lunar_last_year_week_key,
    lunar_2w_bef,
    lunar_2w_aft,
    lunar_6w_bef,
    lunar_6w_aft,
    last_year_lunar_sales_qty_1m_avg,
    last_year_lunar_sales_qty_1m_sum,
    last_year_lunar_ratio_1m,
    last_year_lunar_sales_qty_3m_sum,
    last_year_lunar_sales_qty_3m_avg,
    last_year_lunar_ratio_3m,
    last_year_dm_sales_vrai_exact,
    vrai_exact_or_lunar_1m,
    vrai_exact_or_lunar_3m,
    three_brand_item_list_holding_code,
    uplift,
    4w_sales_4w_bef,
    max(coupon_disc_ratio_avg_max) as coupon_disc_ratio_avg_max
from add_dupplicates
group by item_id, sub_id, store_code, current_dm_theme_id, sub_code
    , city_code, dm_sales_qty, out_of_stock_flag, nl, current_dm_theme_en_desc
    , current_theme_start_date, current_theme_end_date
    , current_dm_psp, current_dm_nsp, current_dm_psp_nsp_ratio
    , current_dm_psp_start_date, current_dm_psp_end_date, current_dm_page_strategy_code
    , current_dm_slot_type_code, current_dm_slot_type_name, current_dm_page_no
    , item_seasonal_code, sub_family_code, family_code, psp_start_week, psp_start_month
    , psp_end_week, last_year_dm_sales, last_year_dm_psp_nsp_ratio
    , last_year_fam_dm_sales_avg, last_5dm_sales_avg, fam_last_5dm_sales_avg, festival_type
    , lunar_last_year_week_key, lunar_2w_bef, lunar_2w_aft, lunar_6w_bef, lunar_6w_aft
    , last_year_lunar_sales_qty_1m_avg, last_year_lunar_sales_qty_1m_sum
    , last_year_lunar_ratio_1m, last_year_lunar_sales_qty_3m_sum, last_year_lunar_sales_qty_3m_avg
    , last_year_lunar_ratio_3m, last_year_dm_sales_vrai_exact, vrai_exact_or_lunar_1m
    , vrai_exact_or_lunar_3m, three_brand_item_list_holding_code, uplift, 4w_sales_4w_bef)

select * from remove_dupplicates_dm;
