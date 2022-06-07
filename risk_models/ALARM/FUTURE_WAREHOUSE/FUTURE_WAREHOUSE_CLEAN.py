import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk-model\risk_model\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
    print(path.dirname(path.dirname(os.getcwd())))
from risk_models import *

ENTRY_HEAD_TBL = "N_EPZ_CUS.GCC_CUSTOM_DUTAPP_HEAD"

class Future_Warehouse(object):
    def __init__(self, org_code, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time

    def clean_layer(self):
        sql_text = f'''
        select 
        t1.ENTRY_ID, 
        t1.D_DATE, 
        t3.WH_REC_PREENT_NO, 
        t1.OWNER_CODE_SCC,
        t1.OWNER_CODE,
        t1.OWNER_NAME, 
        t1.CONSIGN_SCC,
        t1.CONSIGN_CODE,
        t1.CONSIGN_NAME, 
        t1.NOTE_S 
        from 
        (select * from ODS_HEPS.ENTRY_HEAD where NOTE_S LIKE '%期货保税交割%') t1
        left join 
        DW_CUS_RC.TRADE t2 
        on t1.OWNER_CODE_SCC = t2.CREDIT_CODE and t2.LABEL_CODE LIKE '%001%' 
        left join 
        DW_CUS_RC.WAREHOUSE_RECEIPT_BSC t3 
        on t1.ENTRY_ID = t3.RLT_ENTRY_NO 
        '''
        future_warehouse_df = Read_Oracle().read_oracle(sql=sql_text, database='dbalarm')

        print(future_warehouse_df)
        print("------------------------------")

        # 写入数据库
        if future_warehouse_df is not None and len(future_warehouse_df) != 0:
            future_warehouse_df['ID'] = 0
            future_warehouse_df['D_DATE'] = future_warehouse_df['D_DATE'].map(lambda x: x.strftime("%Y%m%d"))
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN', future_warehouse_df, org_code=None, alarm=None)

    def run_clean_layer(self):
        try:
            self.clean_layer()
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
        child_task_id = 'FUTURE_WAREHOUSE_CLEAN'
    else:
        child_task_id = sys.argv[1]
    # org_code, param_json, base_time = read_log_table(child_task_id)
    Future_Warehouse(None, None, child_task_id).run_clean_layer()