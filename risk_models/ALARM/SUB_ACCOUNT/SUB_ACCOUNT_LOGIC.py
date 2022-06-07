import sys, os
from os import path
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth',1000)
if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk-model\risk_model\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
    print(path.dirname(path.dirname(os.getcwd())))
from risk_models import *


class Sub_Account_Logic(object):
    def __init__(self, org_code, base_time, child_task_id, X_PARAM, Y_PARAM):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time
        self.X_PARAM = X_PARAM
        self.Y_PARAM = Y_PARAM

    def deal_dic(self, df):
        temp_dic = {}
        str = "{"
        temp_dic["xParam"] = self.X_PARAM
        temp_dic["yParam"] = self.Y_PARAM
        temp_dic["alarmCount"] = df['ALARM_COUNT']
        temp_dic['detail'] = df['DETAIL']
        for key, value in temp_dic.items():
            str += "\"%s\":\"%s\"," % (key, value)
        return str[: -1] + "}"

    def deal_detail_dic(self, df):
        temp_dic = {}
        str = "{"
        temp_dic['iWhRecNo'] = df['WH_REC_NO_y']
        temp_dic['iType'] = df['TYPE_y']
        temp_dic['eWhReNno'] = df['WH_REC_NO_x']
        temp_dic['eType'] = df['TYPE_x']
        for key, value in temp_dic.items():
            str += "\"%s\":\"%s\"," % (key, value)
        str = str[:-1]
        return str + "},"

    def logic_layer(self):
        param_sql = f'''
        select * from LGSA.PARAM_VALUE_CONFIG where BUS_SON_TYPE = 'crossTradeBigData->subaccountEx->condition'
                    '''
        param_df = Read_Oracle().read_oracle(sql=param_sql, database='dblgsa')
        print(param_df)
        threshold_str = param_df['THRESHOLD'].values[0]
        cardinalNumber = json.loads(threshold_str)['CardinalNumber']
        time_day = json.loads(threshold_str)['T']
        self.Y_PARAM = cardinalNumber
        self.X_PARAM = time_day

        sql_text = f'''select ID, IE_TYPECD, TYPE, ORG_ID, BIZOP_ETPS_SCCD, BIZOP_ETPS_NM, BIZOP_ETPSNO,  RLT_WH_REC_NO, WH_REC_NO, MASTER_CUSCD, DCL_TIME from BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_CLEAN where IE_TYPECD = 'E' '''
        sql_text_other = f'''select ID, IE_TYPECD, TYPE, ORG_ID, BIZOP_ETPS_SCCD, BIZOP_ETPS_NM, BIZOP_ETPSNO, RLT_WH_REC_NO, WH_REC_NO, MASTER_CUSCD, DCL_TIME from BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_CLEAN where IE_TYPECD = 'I' '''
        out_sub_account_df = Read_Oracle().read_oracle(sql=sql_text, database='dbalarm')
        in_sub_account_df = Read_Oracle().read_oracle(sql=sql_text_other, database='dbalarm')

        if out_sub_account_df is not None and in_sub_account_df is not None and len(out_sub_account_df) != 0 and len(in_sub_account_df) != 0:
            res_df_detail = pd.merge(left=out_sub_account_df, right=in_sub_account_df, left_on='RLT_WH_REC_NO', right_on='WH_REC_NO')
            print(res_df_detail)
            print("---"*50)

            #x代表转出方 y代表转入方
            res_df_detail = res_df_detail[['IE_TYPECD_x', 'TYPE_x', 'ORG_ID_x', 'BIZOP_ETPS_SCCD_x', 'BIZOP_ETPS_NM_x', 'BIZOP_ETPSNO_x', 'RLT_WH_REC_NO_x', 'WH_REC_NO_x', 'MASTER_CUSCD_x', 'DCL_TIME_x', 'IE_TYPECD_y', 'TYPE_y', 'ORG_ID_y', 'BIZOP_ETPS_SCCD_y', 'BIZOP_ETPS_NM_y', 'BIZOP_ETPSNO_y', 'RLT_WH_REC_NO_y', 'WH_REC_NO_y', 'MASTER_CUSCD_y', 'DCL_TIME_y']]
            print(res_df_detail)
            print("---" * 50)
            res_df_detail['DETAIL'] = res_df_detail.apply(lambda x: self.deal_detail_dic(x), axis=1)

            I_res_df = res_df_detail[['IE_TYPECD_x', 'TYPE_x', 'ORG_ID_x', 'BIZOP_ETPS_SCCD_x', 'BIZOP_ETPSNO_x', 'BIZOP_ETPS_NM_x', 'RLT_WH_REC_NO_x', 'WH_REC_NO_x', 'MASTER_CUSCD_x', 'DCL_TIME_x', 'DETAIL']]
            E_res_df = res_df_detail[['IE_TYPECD_y','TYPE_y', 'ORG_ID_y', 'BIZOP_ETPS_SCCD_y', 'BIZOP_ETPS_NM_y', 'BIZOP_ETPSNO_y', 'RLT_WH_REC_NO_y', 'WH_REC_NO_y', 'MASTER_CUSCD_y', 'DCL_TIME_y', 'DETAIL']]
            I_res_df.columns = ['IE_TYPECD', 'TYPE', 'ORG_ID', 'BIZOP_ETPS_SCCD', 'BIZOP_ETPSNO', 'BIZOP_ETPS_NM',  'RLT_WH_REC_NO', 'WH_REC_NO', 'MASTER_CUSCD', 'DCL_TIME', 'DETAIL']
            E_res_df.columns = ['IE_TYPECD', 'TYPE', 'ORG_ID', 'BIZOP_ETPS_SCCD', 'BIZOP_ETPS_NM', 'BIZOP_ETPSNO', 'RLT_WH_REC_NO', 'WH_REC_NO', 'MASTER_CUSCD', 'DCL_TIME', 'DETAIL']
#
            res_df = E_res_df
            res_df.rename(columns={"BIZOP_ETPS_SCCD": "CORP_CREDIT_CODE"}, inplace=True)
            res_df.rename(columns={"BIZOP_ETPS_NM": "CORP_NAME"}, inplace=True)
            res_df.rename(columns={"BIZOP_ETPSNO": "TRADE_CODE"}, inplace=True)
            res_df.rename(columns={"ORG_ID": "BUSINESS_NO"}, inplace=True)

            static_df = res_df.groupby(['BUSINESS_NO', 'CORP_CREDIT_CODE', 'CORP_NAME', 'TRADE_CODE']).agg({'BUSINESS_NO': 'count', 'DETAIL': 'sum'})
            static_df.columns = ['ALARM_COUNT', 'DETAIL']
            static_df = static_df[static_df['ALARM_COUNT'] >= int(self.Y_PARAM)]
            # static_df = static_df[static_df['ALARM_COUNT'] >= 1]
            print(static_df)
            print("*" * 50)
            if len(static_df) != 0:
                static_df['DETAIL'] = static_df['DETAIL'].map(lambda x: "[" + x[: -1] + "]")
                static_df['ID'] = 0
                # static_df['LABEL'] = static_df['ALARM_COUNT'].map(lambda x: '子账户退库异常预警' if x > int(self.Y_PARAM) else '子账户正常')
                static_df["CONTEXT"] = static_df.apply(lambda x: self.deal_dic(x), axis=1)
                static_df["CUSTOMS_CODE"] = "2249"
                static_df["TYPE_FIRST"] = "GNYTJKYJ"
                static_df["TYPE_SECOND"] = "BSFW"
                static_df["BUSINESS_TYPE"] = "64"
                static_df["ORDER_TYPE"] = "org"
                static_df["RESOLVE_STATUS"] = "0"
                static_df["RISK_LEVEL"] = "3"
                # static_df.fillna('N/A')
                static_df.drop(columns=['DETAIL', 'ALARM_COUNT'], inplace=True)
                static_df = static_df.reset_index()
                print(static_df)

                if static_df is not None:
                    Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_WARAIN_TEMP', static_df, org_code=None, alarm=None)

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
        child_task_id = 'Sub_Account_Logic'
    else:
        child_task_id = sys.argv[1]
    # org_code, param_json, base_time = read_log_table(child_task_id)
    Sub_Account_Logic(None, None, child_task_id, "30", "3").run_logic_layer()