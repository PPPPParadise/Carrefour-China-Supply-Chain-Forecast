import warnings
import datetime

warnings.filterwarnings('ignore')
from impala.dbapi import connect


def read_sql(sql_file_path, project_folder):
    with open(project_folder + "/sql/" + sql_file_path) as f:
        sql = f.read()
    return sql


def run_sql_with_impala(sql):
    print(sql)

    with connect(host='dtla1apps14', port=21050, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019',
                 database='vartefact') as conn:
        curr = conn.cursor()
        curr.execute(sql)


def record_load_result(runDateStr, table_name):
    sql = \
        """
        insert into vartefact.forecast_script_runs
        select now(), '{0}', 'Success', 'Load table {1}', 'Data loading', 
        'Run date:{0}', cast(count(*) as STRING), '', '' as cnt
        from {1} where date_key='{0}'
        """.replace("\n", " ")

    sql = sql.format(runDateStr, table_name)
    run_sql_with_impala(sql)


def data_loading_process(run_date, tomorrow_date, project_folder):
    print('Load data for date', run_date)

    print('Load forecast_item_code_id_stock')
    sql = read_sql('forecast_item_code_id_stock.sql', project_folder)
    run_sql_with_impala(sql.format(run_date))
    record_load_result(run_date, 'vartefact.forecast_item_code_id_stock')

    print('Load forecast_dc_latest_sales')
    sql = read_sql('forecast_dc_latest_sales.sql', project_folder)
    run_sql_with_impala(sql.format(tomorrow_date))
    record_load_result(tomorrow_date, 'vartefact.forecast_dc_latest_sales')

    print('Load forecast_p4cm_store_item')
    sql = read_sql('forecast_p4cm_store_item.sql', project_folder)
    run_sql_with_impala(sql.format(tomorrow_date))
    record_load_result(tomorrow_date, 'vartefact.forecast_p4cm_store_item')

    print('Load forecast_lfms_daily_dcstock')
    sql = read_sql('forecast_lfms_daily_dcstock.sql', project_folder)
    run_sql_with_impala(sql.format(tomorrow_date))
    record_load_result(tomorrow_date, 'vartefact.forecast_lfms_daily_dcstock')
    
    tomorrow_date_dash = datetime.datetime.strptime(tomorrow_date, '%Y%m%d').date().strftime("%Y-%m-%d")
    
    print('Load forecast_nsa_dm_extract_log')
    sql = read_sql('forecast_nsa_dm_extract_log.sql', project_folder)
    run_sql_with_impala(sql.format(tomorrow_date_dash))
    record_load_result(tomorrow_date, 'vartefact.forecast_nsa_dm_extract_log')

    print('All Loaded')
    

def calculate_service_level_process(tomorrow_date, project_folder):
    print('Load service level')

    print('Load service_level_calculation2')
    sql = read_sql('forecast_service_level_by_item.sql', project_folder)
    run_sql_with_impala("drop table if exists vartefact.service_level_calculation2")
    run_sql_with_impala(sql)
    
    print('Load service_level_safety2_vinc')
    sql = read_sql('forecast_service_level.sql', project_folder)
    run_sql_with_impala("drop table if exists vartefact.service_level_safety2_vinc")
    run_sql_with_impala(sql)

    print('All Loaded')
