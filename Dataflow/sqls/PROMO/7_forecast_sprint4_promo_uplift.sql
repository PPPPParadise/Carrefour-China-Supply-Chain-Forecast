-- ============= TO BIG TABLE =============
/*
Input:  {database}.promo_dataset_feat_combine_exact_vrai
        {database}.forecast_trxn_flag_v1_sprint4
        {database}.uplift_promo

Output: {database}.forecast_sprint4_promo_uplift
*/ 

create table {database}.forecast_sprint4_promo_uplift as
with promo_dataset_holding_code as (
    select
        a.*,
        b.three_brand_item_list_holding_code
    from {database}.promo_dataset_feat_combine_exact_vrai as a
    left join (
        select
            max(three_brand_item_list_holding_code) as three_brand_item_list_holding_code,
            item_id,
            sub_id
        from {database}.forecast_trxn_flag_v1_sprint4
        group by item_id, sub_id) as b
        on a.item_id = a.item_id
            and a.sub_id = b.sub_id
)
select
    a.*,
    b.uplift
from promo_dataset_holding_code as a
left join (
    select
        store_code,
        current_dm_theme_id,
        sub_family_code,
        three_brand_item_list_holding_code,
        (sales_qty_ly_dm / datediff(end_stamp, start_stamp)) / nullif((sales_qty_2w_bef/14), 0) as uplift
    from {database}.uplift_promo
    ) as b 
on a.store_code = b.store_code
    and a.current_dm_theme_id = cast(b.current_dm_theme_id as int)
    and a.sub_family_code = b.sub_family_code
    and a.three_brand_item_list_holding_code = b.three_brand_item_list_holding_code
;
-- 68439 / 257171 => 26.6% (not in 2017 : 68439 / 159774 => 42.3%)


