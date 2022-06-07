import pandas as pd
import os
from os import path
from risk_models.config.read_config.read_func import Read_Oracle
from risk_models.config.auth_config.ezpass_table import TableList
from risk_models.config.param_config import params_global
_name_BD_RISK_MODEL_LOG = TableList.BD_RISK_MODEL_LOG.value

root_path = path.dirname(path.dirname(path.dirname(os.getcwd())))

def read_log_table(child_task_id):
    """
    :desc 生产环境读取 BD_RISK_MODEL_LOG
    :param child_task_id
    :return: ORG_CODE 企业code
             PARAM_JSON 参数json
             BASE_TIME  时间戳参数
    """
    sql = """select ORG_CODE, EXEC_PARAM, BASE_TIME from {} WHERE CHILD_TASK_ID = '{}' """ \
        .format(_name_BD_RISK_MODEL_LOG, child_task_id)
    if params_global.is_test:
        df = Read_Oracle().read_oracle(sql=sql, database='dbalarm')
    else:
        df = Read_Oracle().read_oracle(sql=sql, database='dbdm')
    print(df)
    dic = df.to_dict('records')[0]
    print(dic)
    ORG_CODE = dic['ORG_CODE']
    PARAM_JSON = dic['EXEC_PARAM']
    BASE_TIME = dic['BASE_TIME']
    BASE_TIME = BASE_TIME.strftime('%Y-%m-%d %H:%M:%S')

    return ORG_CODE, PARAM_JSON, BASE_TIME


if __name__ == '__main__':
    read_log_table('childtaskidst3002')
