# Table Information in Data Lake

## Carrefour tables

* DC
    * Stock: lfms.daily_dcstock
    * Transaction (received): lfms.daily_dctrxn
    * Shipment: lfms.daily_shipment
    * Order: lfms.ord
* Store
    * Stock: fds.p4cm_daily_stock
    * 

* DM 
    * ods.nsa_dm_theme
    * nsa.dm_extract_log

## Sales forecast tables

* weekly normal:
    * vartefact.result_forecast_10w_on_the_fututre_all
    * vartefact.forecast_weekly_normal_view
* weekly dm:
    * vartefact.promo_sales_order_prediction_by_item_store_dm_all
    * vartefact.forecast_weekly_dm_view
* daily normal:
    * vartefact.forecast_regular_results_week_to_day_original_pred_all
    * vartefact.forecast_daily_normal_view
* daily dm:
    * vartefact.forecast_dm_results_to_day_all
    * vartefact.forecast_daily_dm_view


## Real orders

* DC
    * normal: vartefact.xxx
    * DM: vartefact.xxx
* Store
    * normal: 
    * DM: 


## Order simulations

* 


## Other

* Item details (rotation, etc)
    * vartefact.forecast_store_item_details (lastest version, updated 22 Aug)
    * vartefact.forecast_item_details (old version)
* Service level by item
    * vartefact.service_level_safety2_vinc
* Daily transaction (oos filled with medium sales)
    * vartefact.forecast_sprint4_add_dm_to_daily
* Item/store in scope
    * Item scope: vartefact.forecast_itemid_list_threebrands_sprint4
    * Store scope: vartefact.forecast_store_code_scope_sprint4
* item_id - holding_code mapping
    * vartefact.forecast_itemid_list_threebrands_sprint4