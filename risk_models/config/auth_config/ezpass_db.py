from enum import unique
from risk_models.config.param_config import params_global
"""
here to save all read config for Ezpass database
"""

# @unique
class Authconfig:
    if params_global.is_test:
        # 测试环境cusrc
        ods_oracle_account = 'CUSRC/easipass'
        oracle_database_dbods = '192.168.129.149:1521/test12c'
        oracle_write_account = 'CUSRC:easipass'
        oracle_write_address = '192.168.129.149:1521/?service_name=test12c'
        oracle_write_account_alarm = 'DW_CUS_RC:easipass'
        oracle_write_address_alarm = '192.168.130.225:1521/?service_name=pdbcusdev'

        # # 测试环境 dw
        dw_oracle_account = 'BI_OPER/easipass'
        oracle_database_dbdw = '192.168.129.179:1521/testdw'

        # 测试环境BD_RISK
        ods_oracle_account = 'BD_RISK/easipass'
        oracle_database_dbods = '192.168.129.149:1521/test12c'
        # oracle_write_account = 'BD_RISK:easipass'
        # oracle_write_address = '192.168.129.149:1521/?service_name=test12c'

        dm_oracle_account = 'BD_RISK/infa4A9F'
        oracle_database_dbdm = '192.168.132.52:1521/dbdm'

        gold_oracle_account = 'n_epz_cus/n_epz_cus'
        oracle_database_gold = '192.168.131.125:1521/zmqwdb'

        alarm_oracle_account = 'DW_CUS_RC/easipass'
        oracle_database_alarm = '192.168.130.225:1521/pdbcusdev'

        lgsa_oracle_account = 'lgsa/lgsa'
        oracle_database_lgsa = '192.168.131.125:1521/zmqwdb'

        testlog_oracle_account = 'BD_RISK/testinfaer'
        oracle_database_testlog = '192.168.129.163:1521/TESTDMAPP'
    else:
        # 生产环境 CUSRC
        # ods_oracle_account = 'CUSRC/infa5F1D'
        # oracle_database_dbods = '192.168.132.12:1521/dbods'
        # oracle_write_account = 'CUSRC:infa5F1D'
        # oracle_write_address = '192.168.132.12:1521/?service_name=dbods'

        # 生产环境 BD_RISK
        oracle_write_account = 'BD_RISK:infa4A9F'
        oracle_write_address = '192.168.132.52:1521/?service_name=dbdm'
        # dbods
        ods_oracle_account = 'CUSRC/infa5F1D'
        oracle_database_dbods = '192.168.132.12:1521/dbods'
        # db_dw
        dw_oracle_account = 'BI_OPER/infa6D83'
        oracle_database_dbdw = '192.168.132.17:1521/dbdw'
        # db_dm
        dm_oracle_account = 'BD_RISK/infa4A9F'
        oracle_database_dbdm = '192.168.132.52:1521/dbdm'

        # Mongodb_config
        mongo_read_oper = 'ods_sens_oper:mgoD52F'
        mongo_ip = '192.168.132.74:27017'
        mongo_database = 'ods_sens_zmxpq'
        mongo_table = 'credit'
