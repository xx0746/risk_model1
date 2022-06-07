import sys, os
from os import path
import datetime
if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk-model\risk_model\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
    print(path.dirname(path.dirname(os.getcwd())))
from risk_models import *
from dateutil.relativedelta import *
import os
import cx_Oracle
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class Transfer_Data_Logic(object):
    def __init__(self, org_code, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time

    def logic_layer(self):
        one_delta_months = relativedelta(months=1)
        time_now = datetime.datetime.now()
        one_month_before = time_now - one_delta_months

        # oracle_write_account_alarm = 'DW_CUS_RC:easipass'
        # oracle_write_address_alarm = '192.168.130.225:1521/?service_name=pdbcusdev'
        db = cx_Oracle.connect("DW_CUS_RC", "easipass", "192.168.130.225:1521/pdbcusdev")
        cursor = db.cursor()
        sql_text = f'''
        INSERT INTO DW_CUS_RC.BD_RISK_CROSS_TRADE_WARAIN (
        ID,
        CUSTOMS_CODE,
        CORP_CREDIT_CODE,
        CORP_NAME,
        TRADE_CODE,
        TYPE_FIRST,
        TYPE_SECOND,
        BUSINESS_TYPE,
        ORDER_TYPE,
        BUSINESS_NO,
        LABEL,
        DESCRIBE,
        CONTEXT,
        RESOLVE_STATUS,
        CREATE_TIME,
        UPDATE_TIME,
        RISK_LEVEL,
        RESOLVE_START_DATE,
        RESOLVE_END_DATE,
        RLT_ID,
        AREA)
        SELECT 
        DW_CUS_RC.SEQ_BD_RISK_CROSS_TRADE_WARAIN.NEXTVAL,
        CUSTOMS_CODE,
        CORP_CREDIT_CODE,
        CORP_NAME,
        TRADE_CODE,
        TYPE_FIRST,
        TYPE_SECOND,
        BUSINESS_TYPE,
        ORDER_TYPE,
        BUSINESS_NO,
        LABEL,
        DESCRIBE,
        CONTEXT,
        RESOLVE_STATUS,
        SYSDATE,
        SYSDATE,
        RISK_LEVEL,
        RESOLVE_START_DATE,
        RESOLVE_END_DATE,
        RLT_ID,
        AREA
        FROM DW_CUS_RC.BD_RISK_CROSS_TRADE_WARAIN_TEMP
                    '''
        cursor.execute(sql_text)
        db.commit()
        cursor.execute("TRUNCATE TABLE DW_CUS_RC.BD_RISK_CROSS_TRADE_WARAIN_TEMP")
        db.commit()

        cursor.close()
        db.close()


    def cal_a(self, trade, secure):
        if trade == 'N/A':
            trade = 0
        if secure == 'N/A':
            secure = 0.0000001
        return int(trade) / int(secure)

    def run_logic_layer(self):
        try:
            self.logic_layer()
            exec_status = 1
        except Exception as e:
            errorStr = f'''{e}'''
            errorStr = errorStr.replace("'", " ")
            errorStr = errorStr[-3900:]
            logger.exception(errorStr)
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    if params_global.is_test:
        child_task_id = 'FUTURE_WAREHOUSE_LOGIC'
    else:
        child_task_id = sys.argv[1]
    # org_code, param_json, base_time = read_log_table(child_task_id)
    Transfer_Data_Logic(None, None, child_task_id).run_logic_layer()