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
pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth',1000)

class Future_Warehouse_Logic(object):
    def __init__(self, org_code, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time

    def deal_dic(self, df):
        temp_dic = {}
        str = "{"
        temp_dic['whRecNo'] = df['WH_REC_PREENT_NO']
        temp_dic['ownerCode'] = df['CONSIGN_SCC']
        temp_dic['ownerName'] = df['CONSIGN_NAME']
        temp_dic['noteS'] = df['NOTE_S']
        for key, value in temp_dic.items():
            str += "\"%s\":\"%s\"," % (key, value)
        return str[: -1] + "}"

    def logic_layer(self):
        sql_text = f'''select ID, ENTRY_ID, D_DATE, WH_REC_PREENT_NO, OWNER_CODE_SCC, OWNER_CODE, OWNER_NAME, CONSIGN_SCC, CONSIGN_CODE, CONSIGN_NAME, NOTE_S from DW_CUS_RC.BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN '''
        future_warehouse_df = Read_Oracle().read_oracle(sql=sql_text, database='dbalarm')
        future_warehouse_df['WARNING_TIME'] = datetime.datetime.now()
        # future_warehouse_df['LABEL'] = '预警'
        print(future_warehouse_df)
        print("---------------------------------")

        if future_warehouse_df is not None and len(future_warehouse_df) != 0:
            collect_df = pd.DataFrame()
            collect_df["ID"] = 0
            collect_df["CONTEXT"] = future_warehouse_df.apply(lambda x: self.deal_dic(x), axis=1)
            collect_df["CORP_CREDIT_CODE"] = future_warehouse_df["OWNER_CODE_SCC"]
            collect_df["CUSTOMS_CODE"] = "2249"
            collect_df["TYPE_FIRST"] = "GNYTJKYJ"
            collect_df["TYPE_SECOND"] = "BSFW"
            collect_df["BUSINESS_TYPE"] = "63"
            collect_df["ORDER_TYPE"] = "entry"
            collect_df["BUSINESS_NO"] = future_warehouse_df["ENTRY_ID"]
            collect_df["RESOLVE_STATUS"] = "0"
            collect_df["RISK_LEVEL"] = "3"
            # collect_df["LABEL"] = "期货保税交割预警"
            collect_df["CORP_NAME"] = future_warehouse_df["OWNER_NAME"]
            collect_df["TRADE_CODE"] = future_warehouse_df["OWNER_CODE"]
            print(collect_df)
            if collect_df is not None:
                Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_WARAIN_TEMP', collect_df, org_code=None, alarm=None)


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
    Future_Warehouse_Logic(None, None, child_task_id).run_logic_layer()