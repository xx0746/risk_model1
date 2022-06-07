import cx_Oracle as cx
import pandas as pd
import os
from loguru import logger
import time
pd.set_option('display.max_columns', None)
from risk_models.config.auth_config import ezpass_db
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class Read_Oracle(object):
    def __init__(self):
        self.link = None
        self.account = None

    def read_oracle_df(self, sql):
        print(self.account)
        print(self.link)
        conn = cx.connect(self.account+'@'+self.link, encoding='utf8')
        data_gen = pd.read_sql(con=conn, sql=sql, chunksize=100000)
        gen_list = []
        for gen in data_gen:
            gen_list.append(gen)
        if len(gen_list) ==0:
            logger.warning('Sql cannot find table, please check database')
            data_df = None
        else:
            data_df = pd.concat(gen_list)
        return data_df

    @logger.catch()
    def read_oracle(self, sql, database):
        start_time = time.time()
        """
        :arg
            :param sql : 读取的select 脚本
            :param databse ：目标database ； dbods、dbdw、dbdm
        :return
            返回查询的结果dataframe
        """
        sql = sql
        if database == 'dbods':
            self.link = ezpass_db.Authconfig.oracle_database_dbods
            self.account = ezpass_db.Authconfig.ods_oracle_account
        elif database == 'dbdw':
            self.link = ezpass_db.Authconfig.oracle_database_dbdw
            self.account = ezpass_db.Authconfig.dw_oracle_account
        elif database == 'dbdm':
            self.link = ezpass_db.Authconfig.oracle_database_dbdm
            self.account = ezpass_db.Authconfig.dm_oracle_account
        elif database == 'dbgold':
            self.link = ezpass_db.Authconfig.oracle_database_gold
            self.account = ezpass_db.Authconfig.gold_oracle_account
        elif database == 'dbalarm':
            self.link = ezpass_db.Authconfig.oracle_database_alarm
            self.account = ezpass_db.Authconfig.alarm_oracle_account
        elif database == 'dblgsa':
            self.link = ezpass_db.Authconfig.oracle_database_lgsa
            self.account = ezpass_db.Authconfig.lgsa_oracle_account
        elif database == 'testlog':
            self.link = ezpass_db.Authconfig.oracle_database_testlog
            self.account = ezpass_db.Authconfig.testlog_oracle_account
        else:
            logger.info('read_oracle config error, please match your input sql and db')
            return

        data_df = self.read_oracle_df(sql)
        if data_df is None:
            data_df = pd.DataFrame()
        if data_df is not None:
            data_df = data_df.fillna('N/A')
        end_time = time.time()
        consum_time = end_time - start_time
        logger.info('Read Table successfully! , Total read time spent {}s'.format(str(consum_time)[:5]))
        return data_df


if __name__ == '__main__':
    sql='select * from N_EPZ_CUS.ORGANIZATION'
    df = Read_Oracle().read_oracle(sql=sql,database='dbgold')
    print(df.head())