-- =======================================
-- ==         FESTIVAL + LUNAR          ==
-- =======================================
/*
    Input:
        {database}.forecast_sprint4_promo_past_features
        {database}.chinese_festivals
        ods.dim_calendar
    Output: {database}.forecast_sprint4_festival_lunar_feat
*/

create table {database}.forecast_sprint4_festival_lunar_feat as
with festival_type_added as (
    select
        a.*,
        b.festival_type
    from {database}.forecast_sprint4_promo_past_features as a
    left join {database}.chinese_festivals as b
    on concat(substring(a.current_dm_psp_start_date,1,4), 
            substring(a.current_dm_psp_start_date,6,2), 
            substring(a.current_dm_psp_start_date,9,2)) < b.date_key
        and concat(substring(a.current_dm_psp_end_date,1,4), 
            substring(a.current_dm_psp_end_date,6,2), 
            substring(a.current_dm_psp_end_date,9,2)) > b.date_key
),

unique_festival as (
    select
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
        max(festival_type) as festival_type
    from festival_type_added
    group by item_id, sub_id, store_code, current_dm_theme_id, sub_code
    , city_code, dm_sales_qty, out_of_stock_flag, nl, current_dm_theme_en_desc
    , current_theme_start_date, current_theme_end_date
    , current_dm_psp, current_dm_nsp, current_dm_psp_nsp_ratio
    , current_dm_psp_start_date, current_dm_psp_end_date, current_dm_page_strategy_code
    , current_dm_slot_type_code, current_dm_slot_type_name, current_dm_page_no
    , item_seasonal_code, sub_family_code, family_code, psp_start_week, psp_start_month
    , psp_end_week, last_year_dm_sales, last_year_dm_psp_nsp_ratio
    , last_year_fam_dm_sales_avg, last_5dm_sales_avg, fam_last_5dm_sales_avg
),

week_mapping as (
    select
        a.week_key,
        min(b.week_key) as lunar_last_year_week_key
    from (  select
                week_key,
                evl_lunar_key
            from ods.dim_calendar) as a
    left join (
        select
            date_key,
            week_key
        from ods.dim_calendar) as b
    on a.evl_lunar_key = b.date_key
    group by week_key
),

fill_forward as (
    select
        week_key,
        max(lunar_last_year_week_key) over (order by week_key rows between unbounded preceding and current row) 
            as lunar_last_year_week_key
    from week_mapping
    where week_key >= '201701'
),

last_year_lunar_added as (
    select
        a.*,
        b.lunar_last_year_week_key
    from unique_festival as a
    left join
    fill_forward as b
    on a.psp_start_week = b.week_key
),

lunar_window as (
    select
        concat(cast(year(date_sub(min(date_value), 15)) as string),
                lpad(cast(weekofyear(date_sub(min(date_value), 15)) as string),2,'0')
                ) as lunar_2w_bef,
        concat(cast(year(date_add(min(date_value), 15)) as string),
                lpad(cast(weekofyear(date_add(min(date_value), 15)) as string),2,'0')
                ) as lunar_2w_aft,
        concat(cast(year(date_sub(min(date_value), 45)) as string),
                lpad(cast(weekofyear(date_sub(min(date_value), 45)) as string),2,'0')
                ) as lunar_6w_bef,
        concat(cast(year(date_add(min(date_value), 45)) as string),
                lpad(cast(weekofyear(date_add(min(date_value), 45)) as string),2,'0')
                ) as lunar_6w_aft,
        week_key
from ods.dim_calendar
group by week_key
),

lunar_window_added as (
    select
        a.*,
        b.lunar_2w_bef,
        b.lunar_2w_aft,
        b.lunar_6w_bef,
        b.lunar_6w_aft
    from last_year_lunar_added as a
    left join lunar_window as b
    on a.lunar_last_year_week_key = b.week_key
),

all_dm_last_year_lunar as (
    select
        a.*,
        b.dm_sales_qty as last_year_lunar_sales_qty,
        b.current_dm_psp_nsp_ratio as last_year_lunar_ratio
    from lunar_window_added as a
    left join lunar_window_added as b
    on a.lunar_2w_bef <= b.psp_start_week
        and a.lunar_2w_aft >= b.psp_start_week
        and a.item_id = b.item_id
        and a.sub_id = b.sub_id
        and a.store_code = b.store_code
),

remove_duplicates as (
select
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
    avg(last_year_lunar_sales_qty) as last_year_lunar_sales_qty_1m_avg,
    sum(last_year_lunar_sales_qty) as last_year_lunar_sales_qty_1m_sum,
    avg(last_year_lunar_ratio) as last_year_lunar_ratio_1m
from all_dm_last_year_lunar
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
),
--  /  => % coverage all scope
--  /  => % without 2017

all_dm_last_year_lunar_3m as (
select
    a.*,
    b.dm_sales_qty as last_year_lunar_sales_qty,
    b.current_dm_psp_nsp_ratio as last_year_lunar_ratio
from remove_duplicates as a
left join remove_duplicates as b
on a.lunar_6w_bef <= b.psp_start_week
    and a.lunar_6w_aft >= b.psp_start_week
    and a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and a.store_code = b.store_code
),

remove_duplicates_bis as (
select
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
    sum(last_year_lunar_sales_qty) as last_year_lunar_sales_qty_3m_sum,
    avg(last_year_lunar_sales_qty) as last_year_lunar_sales_qty_3m_avg,
    avg(last_year_lunar_ratio) as last_year_lunar_ratio_3m
from all_dm_last_year_lunar_3m
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
    , last_year_lunar_ratio_1m
)
--  /  => % coverage all scope
--  /  => % without 2017

select * from remove_duplicates_bis;
-- 311716 no duplicates

