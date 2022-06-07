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


class Sub_Account(object):
    def __init__(self, org_code, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time

    def clean_layer(self):
        param_sql = f'''\
                select * from LGSA.PARAM_VALUE_CONFIG where BUS_SON_TYPE = 'crossTradeBigData->subaccountEx->condition'
                            '''
        param_df = Read_Oracle().read_oracle(sql=param_sql, database='dblgsa')
        print(param_df)
        threshold_str = param_df['THRESHOLD'].values[0]
        cardinalNumber = json.loads(threshold_str)['CardinalNumber']
        time_day = json.loads(threshold_str)['T']
        time_day_int = int(time_day)
        delta_days = datetime.timedelta(days=time_day_int)
        time_start = datetime.datetime.now() - delta_days

        sql_text = f'''select T1.ID, T1.OPT_STATUS, T1.IN_EXP_TYPE, T1.IE_TYPECD, T2.TYPE, T1.ORG_ID, T1.BIZOP_ETPS_SCCD, T1.BIZOP_ETPS_NM, T1.BIZOP_ETPSNO, T1.RLT_WH_REC_NO, T1.WH_REC_NO, T1.MASTER_CUSCD, T1.DCL_TIME from DW_CUS_RC.WAREHOUSE_RECEIPT_BSC T1 LEFT JOIN DW_CUS_RC.ORGANIZATION T2 ON T1.ORG_ID = T2.ORG_CODE where T1.OPT_STATUS = '5' AND T1.IN_EXP_TYPE = '4' AND T1.IE_TYPECD = 'E' AND T2.TYPE = '24' and to_char(T1.DCL_TIME, 'yyyymmdd') >= '{time_start.strftime("%Y%m%d")}' '''
        sql_text_other = f'''select T1.ID, T1.OPT_STATUS, T1.IN_EXP_TYPE, T1.IE_TYPECD, T2.TYPE, T1.ORG_ID, T1.BIZOP_ETPS_SCCD, T1.BIZOP_ETPS_NM, T1.BIZOP_ETPSNO, T1.RLT_WH_REC_NO, T1.WH_REC_NO, T1.MASTER_CUSCD, T1.DCL_TIME from DW_CUS_RC.WAREHOUSE_RECEIPT_BSC T1 LEFT JOIN DW_CUS_RC.ORGANIZATION T2 ON T1.ORG_ID = T2.ORG_CODE where T1.OPT_STATUS = '5' AND T1.IN_EXP_TYPE = '4' AND T1.IE_TYPECD = 'I' AND T2.TYPE != '24' and to_char(T1.DCL_TIME, 'yyyymmdd') >= '{time_start.strftime("%Y%m%d")}' '''
        out_sub_account_df = Read_Oracle().read_oracle(sql=sql_text, database='dbalarm')
        in_sub_account_df = Read_Oracle().read_oracle(sql=sql_text_other, database='dbalarm')

        # 写入数据库
        if out_sub_account_df is not None and len(out_sub_account_df) != 0:
            out_sub_account_df = out_sub_account_df.drop(columns=['OPT_STATUS', 'IN_EXP_TYPE'])
            out_sub_account_df['DCL_TIME'] = out_sub_account_df['DCL_TIME'].map(lambda x: x.strftime("%Y-%m-%d"))
            print(out_sub_account_df)
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_CLEAN', out_sub_account_df, org_code=None, alarm=None)

        if in_sub_account_df is not None and len(in_sub_account_df) != 0:
            in_sub_account_df = in_sub_account_df.drop(columns=['OPT_STATUS', 'IN_EXP_TYPE'])
            in_sub_account_df['DCL_TIME'] = in_sub_account_df['DCL_TIME'].map(lambda x: x.strftime("%Y-%m-%d"))
            print(in_sub_account_df)
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_CLEAN', in_sub_account_df, org_code=None, alarm=None)

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
        child_task_id = 'Sub_Account_CLEAN'
    else:
        child_task_id = sys.argv[1]
    # org_code, param_json, base_time = read_log_table(child_task_id)
    Sub_Account(None, None, child_task_id).run_clean_layer()