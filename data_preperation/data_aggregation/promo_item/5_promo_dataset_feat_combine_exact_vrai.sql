-- ========== ADDING VRAI EXACT + COMBINATION ==========
-- combination of the new exact last year sales with lunar sales
/*
Input:  
    {database}.forecast_sprint4_festival_lunar_feat 
    {database}.last_year_dm_sales_vrai_exact
Output:
    {database}.promo_dataset_feat_combine_exact_vrai
*/ 

create table {database}.promo_dataset_feat_combine_exact_vrai as
with vrai_exact_added as (
    select
        a.*,
        b.sales_qty as last_year_dm_sales_vrai_exact
    from {database}.forecast_sprint4_festival_lunar_feat as a
    left join {database}.last_year_dm_sales_vrai_exact as b
    on a.item_id = b.item_id
        and a.sub_id = b.sub_id
        and a.store_code = b.store_code
        and a.current_dm_theme_id = cast(b.current_dm_theme_id as int)
)

select
    *,
    case when last_year_dm_sales_vrai_exact is not null then cast(last_year_dm_sales_vrai_exact as decimal(38,6)) else last_year_lunar_sales_qty_1m_avg end as vrai_exact_or_lunar_1m,
    case when last_year_dm_sales_vrai_exact is not null then cast(last_year_dm_sales_vrai_exact as decimal(38,6)) else last_year_lunar_sales_qty_3m_avg end as vrai_exact_or_lunar_3m
from vrai_exact_added;
--  /  exact coverage

--  /  => % vrai_exact_or_lunar_1m (no 2017 : %)
--  /  => % vrai_exact_or_lunar_3m (no 2017 : %)
