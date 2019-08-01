/* ================================================================
                              Module 3
                            BP dataflow
    Step 7: Applying flag to transaction table + correct errors
===================================================================
*/

-- Input: {database}.forecast_trxn_v7_full_item_id_sprint4
--        {database}.forecast_trxn_v7_full_item_id_sprint4_group_id
--        {database}.shipment_scope_map_corrected
--        {database}.grouped_to_be_shipment_groupped_0712
--        {database}.forecast_item_store_perc_flagged
-- Output:{database}.forecast_trxn_flag_v1_sprint4
-- Usage: flag planned BPs in transaction table then remove it
--        and correct when link error is too high


create table {database}.forecast_trxn_flag_v1_sprint4 as
with group_id_info as
-- adding groupid for each trxn (all trxn)
(select
    a.*,
    b.group_id
from {database}.forecast_trxn_v7_full_item_id_sprint4 as a
left join
(select
    item_id,
    sub_id,
    store_code,
    ticket_id,
    max(group_id) as group_id
from {database}.forecast_trxn_v7_full_item_id_sprint4_group_id_new
group by item_id, sub_id, store_code, ticket_id) as b
on a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and a.store_code = b.store_code
    and a.ticket_id = b.ticket_id),

planned_identified as (
-- adding flag + delivery information for each group flagged
select
    a.*,
    b.planned_bp,
    b.delivery_date,
    b.delivery_qty_sum
from group_id_info as a
left join
(select
    item_id,
    sub_id,
    store_code,
    group_id,
    max(delivery_date) as delivery_date,
    max(delivery_qty_sum) as delivery_qty_sum,
    1 as planned_bp
from {database}.grouped_to_be_shipment_groupped_0712   -- last version based on Python
group by group_id, item_id, sub_id, store_code) as b
on a.group_id = cast(b.group_id as string)
    and a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and cast(a.store_code as int) = b.store_code),

distinct_selected as
-- drop dupplicates in link table
(select distinct
    item_id,
    sub_id,
    store_code,
    group_id,
    sales_qty,
    delivery_date,
    delivery_qty_sum
from {database}.grouped_to_be_shipment_groupped_0712),

ratio_per_item_store_group as
(select
    item_id,
    sub_id,
    store_code,
    group_id,
    sum(sales_qty) as sales_qty,
    max(delivery_qty_sum) as delivery_qty_sum,
    sum(sales_qty) / max(delivery_qty_sum) as ratio
from distinct_selected
group by item_id, sub_id, store_code, group_id),

ratio_per_item as
(select
    item_id,
    sub_id,
    store_code,
    sum(sales_qty) as sales_qty_flagged,
    sum(delivery_qty_sum) as delivery_qty_sum,
    sum(sales_qty) / sum(delivery_qty_sum) * 100 as perc_flagged
from ratio_per_item_store_group
group by item_id, sub_id, store_code),

too_flagged_identified as
-- identify groups with too high error
(select
    a.*,
    b.too_flagged
from planned_identified as a
left join
(select
    *,
    1 as too_flagged
from ratio_per_item
where perc_flagged > 20000) as b  -- threshold to add back links with high error
on a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and cast(a.store_code as int) = b.store_code),

planned_bp_corrected_added as
-- change back flag to 0 if too much exclusion
(select
    *,
    case when (too_flagged = 1 and delivery_date is not null) then 0 else ifnull(planned_bp, 0) end as planned_bp_corrected
from too_flagged_identified)

select
    three_brand_item_list_id,
    three_brand_item_list_holding_code,
    date_key,
    trxn_date,
    year_key,
    month_key,
    week_key,
    trxn_year,
    trxn_month,
    trxn_week,
    trxn_time,
    ticket_id,
    line_num,
    store_code,
    pos_group,
    sales_type,
    city_code,
    territory_code,
    item_code,
    sub_code,
    item_id,
    sub_id,
    unit_code,
    sales_qty,
    sales_amt,
    ext_amt,
    reward_point,
    reward_amt,
    coupon_disc,
    promo_disc,
    mpsp_disc,
    channel,
    bp_flag,
    current_dm_theme_id,
    current_dm_prom_type,
    current_dm_theme_en_desc,
    current_theme_start_date,
    current_theme_end_date,
    current_city_store_code,
    current_dm_page_no,
    current_dm_nsp,
    current_dm_psp,
    current_dm_psp_start_date,
    current_dm_psp_end_date,
    current_dm_nl,
    current_dm_page_strategy_code,
    current_dm_slot_type_code,
    current_dm_dc_flag,
    current_dm_msp,
    current_dm_msp_get,
    current_dm_msp_end_date,
    ind_current_on_dm,
    next_dm_theme_id,
    next_dm_start_date,
    time_to_next_dm_days,
    next_dm_end_date,
    next_dm_nl,
    next_dm_psp,
    next_dm_prom_type,
    next_dm_nsp,
    next_dm_page_strategy,
    next_dm_page_strategy_name,
    next_dm_slot_type_code,
    next_dm_slot_type_name,
    next_dm_page_no,
    next_dm_msp,
    next_dm_v5_extract_datetime,
    family_code,
    sub_family_code,
    item_seasonal_code,
    ind_holiday
from planned_bp_corrected_added
where planned_bp_corrected != 1;
