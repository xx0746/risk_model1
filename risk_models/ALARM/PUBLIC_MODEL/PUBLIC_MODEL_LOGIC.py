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


class PUBLIC_MODEL_Logic(object):
    def __init__(self, org_code, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time
        self.threshold = 50000000
    def deal_dic(self, df):
        temp_dic = {}
        str = "{"
        temp_dic['iEAmount'] = df['TRADE_VALUE']
        temp_dic['securityNum'] = df['YL_INS']
        temp_dic['publicA'] = df['PUBLIC_A']
        temp_dic['threshold'] = self.threshold
        for key, value in temp_dic.items():
            str += "\"%s\":\"%s\"," % (key, value)
        return str[: -1] + "}"

    def logic_layer(self):
        one_delta_months = relativedelta(months=1)
        time_now = datetime.datetime.now()
        one_month_before = time_now - one_delta_months
        sql_text = f'''
        select T1.I_E_FLAG, T1.OWNER_CODE_SCC AS CREDIT_CODE, T1.OWNER_CODE, T1.OWNER_NAME, nvl(SUM(T2.RMB_PRICE), 0) as TRADE_VALUE
        from ODS_HEPS.ENTRY_HEAD T1
        LEFT JOIN ODS_HEPS.ENTRY_LIST T2
        ON T1.ENTRY_ID = T2.ENTRY_ID
        WHERE to_char(T1.D_DATE, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' AND T1.ISCURRENT = 1 AND T1.ISDELETED = 0 AND T2.ISCURRENT = 1 AND T2.ISDELETED = 0
        GROUP BY T1.I_E_FLAG, T1.OWNER_CODE_SCC, T1.OWNER_CODE, T1.OWNER_NAME
                    '''

        sql_text_none = f'''
                select T1.I_E_FLAG, T1.OWNER_CODE_SCC AS CREDIT_CODE, T1.OWNER_CODE, T1.OWNER_NAME, 0 AS TRADE_VALUE
                from ODS_HEPS.ENTRY_HEAD T1
                WHERE to_char(T1.D_DATE, 'yyyymmdd') >= '{one_month_before.strftime(
            "%Y%m%d")}' AND T1.ISCURRENT = 1 AND T1.ISDELETED = 0
                GROUP BY T1.I_E_FLAG, T1.OWNER_CODE_SCC, T1.OWNER_CODE, T1.OWNER_NAME
                        '''

        sql_text_secure = f'''
                select T1.I_E_FLAG, T1.OWNER_CODE_SCC AS CREDIT_CODE, T1.OWNER_CODE, T1.OWNER_NAME, T2.YL_INS
                from ODS_HEPS.ENTRY_HEAD T1
                LEFT JOIN DW_CUS_RC.T_ANNUAL_SOCIALSECURITY T2
                ON T1.CONSIGN_SCC = T2.COMPANY_ID
                WHERE to_char(T1.D_DATE, 'yyyymmdd') >= '{one_month_before.strftime(
    "%Y%m%d")}' AND T1.ISCURRENT = 1 AND T1.ISDELETED = 0
                        '''
        trade_df = Read_Oracle().read_oracle(sql=sql_text, database='dbalarm')
        if trade_df is None:
            trade_df = Read_Oracle().read_oracle(sql=sql_text_none, database='dbalarm')

        secure_df = Read_Oracle().read_oracle(sql=sql_text_secure, database='dbalarm')

        if secure_df is not None and len(secure_df) != 0:
            res_df = pd.merge(trade_df, secure_df, on=["CREDIT_CODE", "OWNER_NAME", "OWNER_CODE", "I_E_FLAG"])
            res_df['PUBLIC_A'] = res_df.apply(lambda x: self.cal_a(x['TRADE_VALUE'], x['YL_INS']), axis=1)
            res_df = res_df[res_df['PUBLIC_A'] > 50000000.0]
            print(res_df)

            if res_df is not None and len(res_df) != 0:
                collect_df = pd.DataFrame()
                collect_df["ID"] = 0
                collect_df["CONTEXT"] = res_df.apply(lambda x: self.deal_dic(x), axis=1)
                collect_df["CORP_CREDIT_CODE"] = res_df["CREDIT_CODE"]
                collect_df["CUSTOMS_CODE"] = "2249"
                collect_df["TYPE_FIRST"] = "GGQK"
                collect_df["TYPE_SECOND"] = "GGFX"
                collect_df["BUSINESS_TYPE"] = "69"
                collect_df["ORDER_TYPE"] = "trade"
                collect_df["BUSINESS_NO"] = res_df["CREDIT_CODE"]
                collect_df["RESOLVE_STATUS"] = "0"
                collect_df["RISK_LEVEL"] = "3"
                collect_df["CORP_NAME"] = res_df["OWNER_NAME"]
                collect_df["TRADE_CODE"] = res_df["OWNER_CODE"]

                # 写入数据库
                if collect_df is not None:
                    Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_WARAIN_TEMP', collect_df, org_code=None, alarm=None)

    def cal_a(self, trade, secure):
        if trade == 'N/A':
            trade = 0
        if secure == 'N/A':
            secure = 0.0000001
        return float(trade) / float(secure)

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
    PUBLIC_MODEL_Logic(None, None, child_task_id).run_logic_layer()