import cx_Oracle as cx
import pandas as pd
import os
from loguru import logger
from sqlalchemy import types, create_engine
from risk_models.config.auth_config import ezpass_db
from risk_models.config.read_config.read_func import Read_Oracle
import datetime
import time
from sqlalchemy.types import *

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class Write_Oracle_Alarm(object):
    def __init__(self):
        # TODO 需要修改
        self.dmlink = ezpass_db.Authconfig.oracle_database_dbdm
        self.dwlink = ezpass_db.Authconfig.oracle_database_dbdw
        self.odslink = ezpass_db.Authconfig.oracle_database_dbods
        self.account = ezpass_db.Authconfig.ods_oracle_account
        self.write_account = ezpass_db.Authconfig.oracle_write_account_alarm
        self.write_address = ezpass_db.Authconfig.oracle_write_address_alarm
        # 目前只考虑一个conn，以后有多个写入地址再加
        self.conn = create_engine(f"oracle+cx_oracle://{self.write_account}@{self.write_address}",
                                  encoding='utf-8', convert_unicode=True)
        self.conn.autocommit = True

    def get_max_index(self, table_name):
        """
        :param table_name: 旧表表名
        :return: 返回最大的ID
        """
        read_sql = f'select max("ID") as id from {table_name.lower()}'
        # todo 这里的读取用到了dbdm是因为切换了生产环境，原本是dbods
        s_id = Read_Oracle().read_oracle(sql=read_sql, database='dbalarm')
        if s_id is None:
            return 0
        else:
            return 0 if s_id['ID'].values[0] == 'N/A' else s_id['ID'].values[0]

    def get_first_line(self, table_name):
        read_sql = f'select * from {table_name.lower()} where rownum = 1 '
        # todo 这里的读取用到了dbdm是因为切换了生产环境，原本是dbods
        old_df = Read_Oracle().read_oracle(sql=read_sql, database='dbdm')
        return old_df

    def clean_index(self, new_df, old_table_name):
        """
        清洗newdf的index
        :param new_df:
        :param old_table_name:
        :return:
        """
        new_df = new_df.reset_index()
        new_df = new_df.drop(axis=1, columns=['ID']).rename(columns={'index': 'ID'})
        max_id = self.get_max_index(old_table_name)
        if max_id is not None:
            new_df.loc[:, 'ID'] = new_df.loc[:, 'ID'].apply(lambda x: x + max_id + 1)
        else:
            pass
        new_df.set_index('ID')

        return new_df

    def add_lastupdate(self, new_df):
        now_time = datetime.datetime.now()
        now_time = datetime.datetime.strftime(now_time, format='%Y-%m-%d %H:%M:%S')
        new_df.loc[:, 'LASTUPDATE'] = now_time
        new_df.loc[:, 'LASTUPDATE'] = pd.to_datetime(new_df.loc[:, 'LASTUPDATE'], format='%Y-%m-%d %H:%M:%S')

        return new_df

    def add_iscurrent(self, new_df):
        new_df.loc[:, 'ISCURRENT'] = 1
        return new_df

    def change_lastupdate(self, old_table_name, org_code, alarm):
        """
        :param old_table_name: 表名
        :param org_code: 企业信用代码
        :param alarm: [model_code, child_mode_code] 主模块id和子模块id
        :return: None
        """
        now_time = datetime.datetime.now()
        now_time = datetime.datetime.strftime(now_time, format='%Y-%m-%d %H:%M:%S')
        if org_code is not None and alarm is None:
            try:
                self.conn.execute(
                    "update {} set LASTUPDATE =to_date('{}','yyyy-mm-dd hh24:mi:ss') where iscurrent = 1 and org_code ='{}'".format(
                        old_table_name, now_time, org_code))
            except Exception as e:
                logger.error(e)
        elif alarm is not None:
            if org_code is not None and len(alarm) == 2:
                model_code = alarm[0]
                child_mode_code = alarm[1]
                if child_mode_code == '':
                    try:
                        self.conn.execute(
                            "update {} set LASTUPDATE =to_date('{}','yyyy-mm-dd hh24:mi:ss') where iscurrent = 1 and org_code ='{}' and model_code = '{}'".format(
                                old_table_name, now_time, org_code, model_code))
                    except Exception as e:
                        logger.error(e)
                else:
                    try:
                        self.conn.execute(
                            "update {} set LASTUPDATE =to_date('{}','yyyy-mm-dd hh24:mi:ss') where iscurrent = 1 and org_code ='{}' and model_code = '{}' and child_model_code ='{}'".format(
                                old_table_name, now_time, org_code, model_code, child_mode_code))
                    except Exception as e:
                        logger.error(e)
            elif org_code is None and len(alarm) == 2:
                model_code = alarm[0]
                child_mode_code = alarm[1]
                if child_mode_code == '':
                    try:
                        self.conn.execute(
                            "update {} set LASTUPDATE =to_date('{}','yyyy-mm-dd hh24:mi:ss') where iscurrent = 1 and model_code = '{}'".format(
                                old_table_name, now_time,  model_code))
                    except Exception as e:
                        logger.error(e)
                else:
                    try:
                        self.conn.execute(
                            "update {} set LASTUPDATE =to_date('{}','yyyy-mm-dd hh24:mi:ss') where iscurrent = 1 and model_code = '{}' and child_model_code ='{}'".format(
                                old_table_name, now_time, model_code, child_mode_code))
                    except Exception as e:
                        logger.error(e)
        elif org_code is None:
            try:
                self.conn.execute(
                    "update {} set LASTUPDATE =to_date('{}','yyyy-mm-dd hh24:mi:ss') where iscurrent = 1 ".format(old_table_name, now_time))
            except Exception as e:
                logger.error(e)
        else:
            logger.warning('change_lastupdate func params error, please check params')

    def change_iscurrent(self, old_table_name, org_code, alarm):
        if org_code is not None and alarm is None:
            try:
                self.conn.execute("update {} set ISCURRENT = 0  where iscurrent = 1 and org_code ='{}'".format(old_table_name, org_code))
            except Exception as e:
                logger.error(e)
        elif alarm is not None:
            if org_code is not None and len(alarm) == 2:
                model_code = alarm[0]
                child_mode_code = alarm[1]
                if child_mode_code == '':
                    try:
                        self.conn.execute(
                            "update {} set ISCURRENT = 0  where iscurrent = 1 and org_code ='{}' and model_code = '{}'".format(old_table_name, org_code, model_code))
                    except Exception as e:
                        logger.error(e)
                else:
                    try:
                        self.conn.execute(
                            "update {} set ISCURRENT = 0  where iscurrent = 1 and org_code ='{}' and model_code = '{}' and child_model_code ='{}'".format(
                                old_table_name, org_code, model_code, child_mode_code))
                    except Exception as e:
                        logger.error(e)
            elif org_code is None and len(alarm) == 2:
                model_code = alarm[0]
                child_mode_code = alarm[1]
                if child_mode_code == '':
                    try:
                        self.conn.execute(
                            "update {} set ISCURRENT = 0 where iscurrent = 1 and model_code = '{}'".format(old_table_name, model_code))
                    except Exception as e:
                        logger.error(e)
                else:
                    try:
                        self.conn.execute(
                            "update {} set ISCURRENT = 0 where iscurrent = 1 and model_code = '{}' and child_model_code ='{}'".format(
                                old_table_name, model_code, child_mode_code))
                    except Exception as e:
                        logger.error(e)
        elif org_code is None:
            try:
                self.conn.execute("update {} set ISCURRENT = 0 where iscurrent = 1 ".format(old_table_name))
            except Exception as e:
                logger.error(e)
        else:
            logger.warning('change_iscurrent_func params error, please check params')

    @logger.catch()
    def write_oracle(self, table_name, new_df, org_code, alarm, iscurrent=True, alarm_code = None):
        """
        风控产品封装的写入接口
        :param iscurrent: 是否需要在插入的时候更新iscurrent
        :param table_name: 需要写入的表名
        :param new_df: 写入的对象dataframe
        :param org_code: 需要更新的企业信用代码
        :param alarm:  [model_code, child_mode_code] 主模块id和子模块id
        :return: None
        """
        if new_df is None:
            return
        start_time = time.time()
        new_df = self.clean_index(new_df, table_name)
        print(new_df)
        new_df = self.add_lastupdate(new_df)
        new_df = self.add_iscurrent(new_df)
        self.change_lastupdate(table_name, org_code, alarm)
        if iscurrent:
            self.change_iscurrent(table_name, org_code, alarm)
        # old_df = self.get_first_line(table_name)

        for key, value in dict(new_df.dtypes).items():
            # 如果不对object的columns进行一次astype（str）就会报错
            if value == 'object':
                new_df.loc[:, '{}'.format(key)] = new_df.loc[:, '{}'.format(key)].astype(str)

        def set_d_type_dict(df):
            type_dict = {}
            for i, j in zip(df.columns, df.dtypes):
                if "object" in str(j):
                    type_dict.update({i: VARCHAR(512)})
                if "float" in str(j):
                    type_dict.update({i: DECIMAL(20, 5)})
                if "int" in str(j):
                    type_dict.update({i: DECIMAL(20)})
            return type_dict

        logger.info('Processing... Writing {} rows into database'.format(len(new_df)))
        d_type = set_d_type_dict(new_df)
        new_df.to_sql(name=table_name.lower(), con=self.conn, if_exists='append', index=False, dtype=d_type, chunksize=2000)

        end_time = time.time()
        consum_time = end_time - start_time
        logger.info('Insert data into {} successfully! Total write time spent {}s'.format(table_name.upper(), str(consum_time)[:5]))
