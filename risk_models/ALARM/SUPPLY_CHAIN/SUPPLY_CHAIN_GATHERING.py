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


class SupplyChain_Logic(object):
    def __init__(self, org_code, base_time, child_task_id):
        self.child_task_id = child_task_id
        '''
        1 表示经营单位
        2 表示收货单位
        3 表示境外发货人
        4 表示报关单位
        '''
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time
        self.columns = ["OPERATION", "RECEIVE", "ABROAD", "CUSTOM"]
        self.weights = [0.2142, 0.2850, 0.3807, 0.1201]
    def logic_layer(self):
        sql_text_1 = f'''
        select T1.CREDIT_CODE as CREDIT_CODE_OPERATION, T1.ENTRY_ID, T1.D_DATE, T1.I_E_FLAG, T1.TOTAL_SCORE as OPERATION, T2.FRN_CONSIGN_CODE AS CREDIT_CODE_ABROAD, 60 AS ABROAD
        FROM DW_CUS_RC.BD_RISK_CROSS_TRADE_RESULT_CREDIT_SCORE_TBL T1
        LEFT JOIN ODS_HEPS.ENTRY_HEAD T2
        ON T1.ENTRY_ID = T2.ENTRY_ID
        WHERE T1.DEAL_TYPE = 1
                    '''
        df_1 = Read_Oracle().read_oracle(sql=sql_text_1, database='dbalarm')
        print(df_1)

        sql_text_2 = f'''select CREDIT_CODE as CREDIT_CODE_RECEIVE, ENTRY_ID, TOTAL_SCORE as RECEIVE
                FROM DW_CUS_RC.BD_RISK_CROSS_TRADE_RESULT_CREDIT_SCORE_TBL 
                WHERE DEAL_TYPE = 2
                            '''
        df_2 = Read_Oracle().read_oracle(sql=sql_text_2, database='dbalarm')
        print(df_2)
        res_df = pd.merge(df_1, df_2, on="ENTRY_ID")

        sql_text_4 = f'''select CREDIT_CODE as CREDIT_CODE_CUSTOM, ENTRY_ID, TOTAL_SCORE as CUSTOM
                FROM DW_CUS_RC.BD_RISK_CROSS_TRADE_RESULT_CREDIT_SCORE_TBL 
                WHERE DEAL_TYPE = 4
                            '''
        df_4 = Read_Oracle().read_oracle(sql=sql_text_4, database='dbalarm')
        print(df_4)
        res_df = pd.merge(res_df, df_4, on="ENTRY_ID")

        res_df_num = len(res_df)
        weights_arr1 = self.copyList(self.weights, res_df_num)
        res_df['TOTAL'] = self.get_weightScore_totalScore(res_df[['OPERATION', 'RECEIVE', 'ABROAD', 'CUSTOM']], weights_arr1, self.columns)

        print(res_df)

        # 写入数据库
        if res_df is not None and len(res_df) != 0:
            res_df['ID'] = 0
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_RESULT_SUPPLY_CHAIN_SCORE_TBL', res_df, org_code=None, alarm=None)

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

    def get_weightScore_totalScore(self, list_df, weights, column_list):
        list_df.columns = column_list
        df_weights = pd.DataFrame(weights, columns=column_list)
        print(df_weights)
        df_res = list_df.mul(df_weights, axis=0)
        sum_res = df_res.sum(axis=1)
        return sum_res

    def copyList(self, tempList, num):
        weights_res_list = []
        for i in range(0, num):
            weights_res_list.append(tempList)
        return weights_res_list

if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    if params_global.is_test:
        child_task_id = 'SupplyChain_Logic'
    else:
        child_task_id = sys.argv[1]
    # org_code, param_json, base_time = read_log_table(child_task_id)
    SupplyChain_Logic(None, None, child_task_id).run_logic_layer()