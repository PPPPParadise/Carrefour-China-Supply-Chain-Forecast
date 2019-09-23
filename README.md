# Carrefour-China-Supply-Chain-Forecast


Folder Structure
--------

| File Path                                         | content                                                               |
| :---------------                                  | :------------------------------------------------------------         |
| ./config                                          | Configs                                                               |
| ./config/setup_config_data                        | Parameters used in setup config                                       |
| ./config/input_config_data                        | Input data in config                                                  |
| ./data_modeling                                   | Code of model training and prediction                                 |
| ./data_modeling/dm_sales                          | Code of DM model training and prediction                              |
| ./data_modeling/normal_sales                      | Code of Normal model training and prediction                          |
| ./data_preperation                                | Code of data processing                                               |
| ./data_preperation/data_aggregation               | SQLs of Data aggregation                                              |
| ./data_preperation/data_aggregation/conversion_week_to_day | SQLs of conversion week to day                               |
| ./data_preperation/data_aggregation/new_item      | SQLs of new item                                                      |
| ./data_preperation/data_aggregation/promo_item    | SQLs of promo item                                                    |
| ./data_preperation/data_aggregation/regular_item  | SQLs of regular item                                                  |
| ./data_preperation/data_aggregation/regular_item/regular_item_source_code | source code of Scala for regular item         |
| ./data_preperation/dataflow_controller            | Controller of data aggregation in Python                              |
| ./demand_forecast_kpi_measurement                 | demand forecast kpi measurement                                       |
| ./demand_planning                                 | demand planning code                                                  |
| ./demand_planning/initiate_env                    | demand planning initiate env                                          |
| ./demand_planning/order_template                  | demand planning order template                                        |
| ./demand_planning/run_scripts                     | demand planning run scripts                                           |
| ./demand_planning/source_code                     | demand planning source code                                           |
| ./documents                                       | Project Documentation                                                 |
| ./workflow_integration                            | Airflow Dags.                                                         |
| ./test                                            | Files for testing                                                     |


