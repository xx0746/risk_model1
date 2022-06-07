import cx_Oracle as cx
import pandas as pd
import os
from loguru import logger
from sqlalchemy import types, create_engine
from risk_models.config.auth_config import ezpass_db
# from risk_models.config.read_config.read_func import Read_Oracle
import time
from risk_models.config.auth_config.ezpass_table import TableList
import datetime
from sqlalchemy.types import *

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
_name_BD_RISK_MODEL_LOG = TableList.BD_RISK_MODEL_LOG.value


class Write_Oracle(object):
    def __init__(self):
        # TODO 因为之前迁移到了dm，然后修改了AUTHCONFIG的emun属性所以没有.value了
        self.dmlink = ezpass_db.Authconfig.oracle_database_dbdm
        self.dwlink = ezpass_db.Authconfig.oracle_database_dbdw
        self.odslink = ezpass_db.Authconfig.oracle_database_dbods
        self.account = ezpass_db.Authconfig.ods_oracle_account
        self.write_account = ezpass_db.Authconfig.oracle_write_account
        self.write_address = ezpass_db.Authconfig.oracle_write_address
        if type == 'testlog':
            self.write_account = 'BD_RISK:testinfaer'
            self.write_address = '192.168.129.163:1521/?service_name=testdmapp'
        # 目前只考虑一个conn，以后有多个写入地址再加
        self.conn = create_engine(f"oracle+cx_oracle://{self.write_account}@{self.write_address}",
                                  encoding='utf-8', convert_unicode=True)
        self.conn.autocommit = True

    @logger.catch()
    def write_oracle(self, log_json,type=None):
        """
        """
        if type == 'testlog':
            self.write_account = 'BD_RISK:testinfaer'
            self.write_address = '192.168.129.163:1521/?service_name=testdmapp'
        start_time = time.time()
        log_message = log_json['LOG_MSG'].replace("'","")
        exec_status = log_json['EXEC_STATUS']
        child_task_id = log_json['CHILD_TASK_ID']

        now_time = datetime.datetime.now()
        update_end_time = datetime.datetime.strftime(now_time, format='%Y-%m-%d %H:%M:%S')

        self.conn.execute("update {} set END_TIME = to_date('{}','yyyy-mm-dd hh24:mi:ss') where child_task_id ='{}'"
                          .format(_name_BD_RISK_MODEL_LOG, update_end_time, child_task_id))
        sql="""update {} set LOG_MSG = '{}' where child_task_id ='{}'""".format(_name_BD_RISK_MODEL_LOG, log_message, child_task_id)
        self.conn.execute(sql)
        self.conn.execute("update {} set EXEC_STATUS = {} where child_task_id ='{}'"
                          .format(_name_BD_RISK_MODEL_LOG, exec_status, child_task_id))
        end_time = time.time()
        consum_time = end_time - start_time

        logger.info(
            f'Updated child_task_id: {child_task_id} log msg into {_name_BD_RISK_MODEL_LOG.upper()} successfully!',
            'Total write time spent {}s'.format(consum_time))