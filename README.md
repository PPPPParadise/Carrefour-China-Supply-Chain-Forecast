# Carrefour-China-Supply-Chain-Forecast


Folder Structure 
--------

| File Path                                         | content                                                       |
| :---------------                                  | :------------------------------------------------------------ |
| ./config                                          | Configs                                                       |
| ./config/execution_parameters                     | Execution_parameters                                          |
| ./config/input_config_data                        | Input_config_data                                             |
| ./data_modeling                                   | Code of model training and prediction                         |
| ./data_modeling/dm_sales                          | Code of DM model training and prediction                      |
| ./data_modeling/normal_sales                      | Code of Normal model training and prediction                  |
| ./data_preperation                                | Code of data processing                                       |
| ./data_preperation/data_aggregation               | SQLs of Data_aggregation                                      |
| ./data_preperation/data_aggregation/conversion_week_to_day | SQLs of conversion_week_to_day                       |
| ./data_preperation/data_aggregation/new_item      | SQLs of new_item                                              |
| ./data_preperation/data_aggregation/promo_item    | SQLs of promo_item                                            |
| ./data_preperation/data_aggregation/regular_item  | SQLs of regular_item                                          |
| ./data_preperation/data_aggregation/regular_item/regular_item_source_code | source code of Scala for regular item |
| ./data_preperation/data_aggregation/new_item      | SQLs of new_item                                              |
| ./data_preperation/dataflow_controller            | Controller of data aggregation in Python                      |
| ./demand_forecast_kpi_measurement                 | demand_forecast_kpi_measurement                               |
| ./demand_planning                                 | demand_planning                                               |
| ./demand_planning/initiate_env                    | demand_planning initiate_env                                  |
| ./demand_planning/order_template                  | demand_planning order_template                                |
| ./demand_planning/run_scripts                     | demand_planning run_scripts                                   |
| ./demand_planning/source_code                     | demand_planning source_code                                   |
| ./documents                                       | documents                                                     |
| ./workflow_integration                            | Airflow Dags.                                                 |
| ./test                                            | Files for testing                                             |


Usage
--------

1. Put the ```forecast_airflow.py``` file in ```./workflow_integration``` into the Airflow Dags folder.
2. Turn it on at Airflow UI