from loguru import logger
import sys
import os
from os import path
import datetime
import pandas as pd
from risk_models.config.write_config.write_log import Write_Oracle
from risk_models.config.auth_config.ezpass_table import TableList

sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
root_path = path.dirname(path.dirname(path.dirname(os.getcwd())))

_name_BD_RISK_MODEL_LOG = TableList.BD_RISK_MODEL_LOG.value


class Risk_logger(object):
    """
    这是该项目的日志模块
    :param child_task_id 子任务id
    :param exec_status 执行情况
    :return
    更新该子任务id的日志数据，并将log message更新在该条数据下
    """
    def __init__(self, child_task_id, exec_status):
        self.child_task_id = child_task_id
        self.exec_status = exec_status
        log_file = 'mylog.log'
        self.log_address = root_path + '//log//{}'.format(log_file)
        logger.remove()
        logger.add(self.log_address,
                   enqueue=True, retention="10 days", backtrace=False, diagnose=False)
        logger.add(sys.stderr)

    def gen_log_json(self):
        log_json = {}
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        logger.info(
            f'updating child_task_id:{self.child_task_id} log msg into {_name_BD_RISK_MODEL_LOG.upper()} successfully!')
        with open(self.log_address, 'r') as fp:
            log_msg = fp.read()
        log_json['LOG_MSG'] = str(log_msg[-1900:])
        log_json['CHILD_TASK_ID'] = self.child_task_id
        log_json['END_TIME'] = now
        log_json['EXEC_STATUS'] = self.exec_status

        return log_json

    def clean_log(self):
        if os.path.exists(self.log_address):
            # 很重要 logger.remove
            logger.remove()
            os.remove(self.log_address)

    @logger.catch()
    def write_log(self):
        log_json = self.gen_log_json()
        try:
            Write_Oracle().write_oracle(log_json,type='testlog')
        except:
            logger.exception('Logger execution error')
        finally:
            self.clean_log()
