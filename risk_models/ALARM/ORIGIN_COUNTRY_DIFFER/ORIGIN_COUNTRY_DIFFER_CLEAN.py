import sys, os
from os import path

from risk_models.config.read_config.read_func import Read_Oracle

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"C:\Users\lenovo\Downloads\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
    print(path.dirname(path.dirname(os.getcwd())))
from risk_models import *


class Origin_Country(object):
    def __init__(self, org_code, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time

    def clean_layer(self):
        sql_text = f'''select T1.ENTRY_ID, T1.I_E_FLAG, T1.I_E_PORT, T1.D_DATE, T1.OWNER_CODE_SCC, T1.OWNER_CODE, T1.OWNER_NAME, T1.CONSIGN_SCC, T1.CONSIGN_CODE, T1.CONSIGN_NAME, T1.TRADE_MODE, T2.G_NAME, T2.G_QTY, T2.ORIGIN_COUNTRY from ODS_HEPS.ENTRY_HEAD T1 join ODS_HEPS.ENTRY_LIST T2 on T1.ENTRY_ID = T2.ENTRY_ID where T1.I_E_FLAG = 'I' AND (T1.TRADE_MODE = '0110' OR T1.TRADE_MODE = '5034') AND T1.I_E_PORT = '2249' '''
        origin_country_df = Read_Oracle().read_oracle(sql=sql_text, database='dbalarm')

        print(origin_country_df)
        if origin_country_df is not None:
            origin_country_df['D_DATE'] = origin_country_df['D_DATE'].map(lambda x: x.strftime("%Y%m%d"))
            origin_country_df = origin_country_df.drop(columns=['I_E_FLAG'])
            origin_country_df['ID'] = 0

            # 去重
            sql2 = '''select distinct ENTRY_ID from DW_CUS_RC.BD_RISK_CROSS_TRADE_RESULT_ORIGIN_COUNTRY_DIFFER_CLEAN'''
            ENTRY_ID_DF = Read_Oracle().read_oracle(sql=sql2, database='dbalarm')

            list_temp = []
            for i in range(ENTRY_ID_DF.shape[0]):
                if i not in list_temp:
                    list_temp.append(ENTRY_ID_DF.iloc[i]['ENTRY_ID'])
            list_temp2 = list(set(list_temp))
            # 提取ENTRY_ID 并去重
            i_list = []
            for i in range(origin_country_df.shape[0]):
                if origin_country_df.iloc[i]['ENTRY_ID'] in list_temp2:
                    i_list.append(i)
            origin_country_df_distinct = origin_country_df.drop(labels=i_list)
            # 获得去重后的DataFrame, origin_country_df_distinct
            # 写入数据库
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_RESULT_ORIGIN_COUNTRY_DIFFER_CLEAN', origin_country_df_distinct, org_code=None, alarm=None)

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
        child_task_id = 'Origin_Country_Differ_CLEAN'
    else:
        child_task_id = sys.argv[1]
    # org_code, param_json, base_time = read_log_table(child_task_id)
    Origin_Country(None, None, child_task_id).run_clean_layer()