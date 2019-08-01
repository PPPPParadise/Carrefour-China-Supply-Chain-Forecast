-- =================================================
-- ==      LAST_YEAR + LAST_5DM [ITEM / FAM]      ==
-- =================================================
/*
Input:
        {database}.forecast_sprint4_dm_agg_v2
        ods.dim_calendar 
Output: {database}.forecast_sprint4_promo_past_features
*/ 
create table {database}.forecast_sprint4_promo_past_features as
-- temp table to add row number
with first_dm_month_1 as (
    select
        *,
        row_number() over(partition by store_code,item_id,sub_id, psp_start_month order by psp_start_week asc) as rownum
    from {database}.forecast_sprint4_dm_agg_v2
),

-- table to select first DM of each month per item_store
first_dm_month_2 as (
    select
        *
    from first_dm_month_1
    where rownum = 1
),

-- adding to big table: last year comparable DM sales + psp_nsp_ratio
last_year_dm as (
    select
        a.*,
        b.dm_sales_qty as last_year_dm_sales,
        b.current_dm_psp_nsp_ratio as last_year_dm_psp_nsp_ratio
    from {database}.forecast_sprint4_dm_agg_v2 as a
    left join (
        select
            item_id,
            sub_id,
            store_code,
            psp_start_month,
            dm_sales_qty,
            current_dm_psp_nsp_ratio
        from first_dm_month_2) as b
        on a.item_id = b.item_id
            and a.sub_id = b.sub_id
            and a.store_code = b.store_code
            and a.psp_start_month = cast((cast(b.psp_start_month as int) + 100) as string)
),
--  /  => % item_store_dm with info from last year dm
--  /  => % with trxn_year > 2017

-- avg sales by family_store in DM per month
fam_dm_avg_sales as (
    select
        family_code,
        store_code,
        psp_start_month,
        avg(dm_sales_qty) as dm_avg_sales
    from last_year_dm
    group by family_code, store_code, psp_start_month
),

-- adding to big table: last year family sales avg
last_year_fam_dm as (
    select
        a.*,
        b.dm_avg_sales as last_year_fam_dm_sales_avg
    from last_year_dm as a
    left join fam_dm_avg_sales as b
    on a.family_code = b.family_code
        and a.store_code = b.store_code
        and a.psp_start_month = cast((cast(b.psp_start_month as int)+100) as string)
),
--  /  => % item_store_dm with last year family info
--  /  => % coverage without 2017

-- add the week_key 6 weeks before
bigtable_week_6bef as (
    select
        a.*,
        b.week_6bef
    from last_year_fam_dm as a
    left join(
        select
            week_key,
            concat(cast(year(date_sub(min(date_value), 42)) as string),
                    lpad(cast(weekofyear(date_sub(min(date_value), 42)) as string),2,'0')) as week_6bef
        from ods.dim_calendar
        group by week_key) as b
        on a.psp_start_week = b.week_key
),

-- adding per item_store all dm before 6 weeks
dm_info_6bef_adding as (
    select
        a.*,
        b.psp_start_week as dm_start_week_6bef,
        b.dm_sales_qty as dm_sales_qty_6bef
    from bigtable_week_6bef as a
    left join bigtable_week_6bef as b
    on a.item_id = b.item_id
        and a.sub_id = b.sub_id
        and a.store_code = b.store_code
        and a.week_6bef > b.psp_start_week
),
-- XX rows because of duplicates from past DM info

-- adding row number: to later drop too old DM info
duplicates_plus_rownum as (
    select
        *,
        row_number() over(partition by store_code,item_id,sub_id,current_dm_theme_id order by dm_start_week_6bef desc) as rownum
    from dm_info_6bef_adding
    ),

-- taking only 5 DM + average on those 5 DM before 6 weeks
big_table_avg_5dm_6bef as (
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
        week_6bef,
        max(dm_start_week_6bef) as last_dm_week_6bef_max,
        avg(dm_sales_qty_6bef) as last_5dm_sales_avg
    from duplicates_plus_rownum
    where rownum between 1 and 5
    group by item_id, sub_id, store_code, current_dm_theme_id, sub_code, 
            city_code, dm_sales_qty, out_of_stock_flag, nl, current_dm_theme_en_desc, 
            current_theme_start_date, current_theme_end_date, 
            current_dm_psp, current_dm_nsp, current_dm_psp_nsp_ratio,
            current_dm_psp_start_date, current_dm_psp_end_date, current_dm_page_strategy_code, 
            current_dm_slot_type_code, current_dm_slot_type_name, current_dm_page_no, 
            item_seasonal_code, sub_family_code, family_code, psp_start_week, psp_start_month,
            psp_end_week, last_year_dm_sales, last_year_dm_psp_nsp_ratio, 
            last_year_fam_dm_sales_avg, week_6bef),
-- 311716 item_store_dm in the big_table (same as beginning = no duplicates)
--  /  => % of coverage of last_5dm_sales_avg

-- adding per family_store all dm before 6 weeks
fam_dm_info_6bef_adding as (
    select
        a.*,
        b.psp_start_week as fam_dm_start_week_6bef,
        b.dm_sales_qty as fam_dm_sales_qty_6bef
    from big_table_avg_5dm_6bef as a
    left join big_table_avg_5dm_6bef as b
    on a.family_code = b.family_code
        and a.store_code = b.store_code
        and a.week_6bef > b.psp_start_week
),
--  rows because of duplicates from past DM info

-- adding row number: to later drop too old family DM info
fam_duplicates_plus_rownum as (
    select
        *,
        row_number() over(partition by store_code,item_id,sub_id,current_dm_theme_id order by fam_dm_start_week_6bef desc) as rownum
    from fam_dm_info_6bef_adding
),

-- taking only 5 DM family_store + average on those 5 DM before 6 weeks
big_table_fam_avg_5dm_6bef as (
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
        -- week_6bef,
        -- last_dm_week_6bef_max,
        last_5dm_sales_avg,
        -- max(fam_dm_start_week_6bef) as fam_last_dm_week_6bef_max,
        avg(fam_dm_sales_qty_6bef) as fam_last_5dm_sales_avg
    from fam_duplicates_plus_rownum
    where rownum between 1 and 5
    group by item_id, sub_id, store_code, current_dm_theme_id, sub_code, 
            city_code, dm_sales_qty, out_of_stock_flag, nl, current_dm_theme_en_desc,
            current_theme_start_date, current_theme_end_date, 
            current_dm_psp, current_dm_nsp, current_dm_psp_nsp_ratio, 
            current_dm_psp_start_date, current_dm_psp_end_date, current_dm_page_strategy_code, 
            current_dm_slot_type_code, current_dm_slot_type_name, current_dm_page_no, 
            item_seasonal_code, sub_family_code, family_code, psp_start_week, psp_start_month, 
            psp_end_week, last_year_dm_sales, last_year_dm_psp_nsp_ratio, 
            last_year_fam_dm_sales_avg, week_6bef, last_dm_week_6bef_max, last_5dm_sales_avg
)
    -- 311716 item_store_dm in the big_table (same as beginning = no duplicates)
    --  /  => % of coverage of fam_last_5dm_sales_avg

select * from big_table_fam_avg_5dm_6bef;
