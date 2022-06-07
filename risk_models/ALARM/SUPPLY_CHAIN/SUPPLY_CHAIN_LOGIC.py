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
    def __init__(self, org_code, base_time, child_task_id, deal_type):
        self.child_task_id = child_task_id
        '''
        1 表示经营单位
        2 表示收货单位
        3 表示境外发货人
        4 表示报关单位
        '''
        self.deal_type = deal_type
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time
        self.columns = ["W1_CUSTOM_WEIGHT", "W2_TAX_CREDIT_WEIGHT", "W3_ADMINISTRATION_WEIGHT", "W4_CRIMINAL_WEIGHT", "W5_LOST_CREDIT_WEIGHT", "W6_EXCUTED_WEIGHT", "W7_NOT_OPERATION_WEIGHT", "W8_SOCIAL_SECURE_WEIGHT",]
        self.weights = [0.6142, 0.3858, 0.1594, 0.4657, 0.2157, 0.1592, 0.6185, 0.3815]
        self.total_columns = ["W1_CREDIT_RISK_WEIGHT", "W2_ENTERPRISE_RISK_WEIGHT", "W3_OTHER_RISK_WEIGHT"]
        self.weights_type1 = [0.3775, 0.4368, 0.1857]
        self.weights_type3 = [0.4152, 0.4802, 0.1046]
        if self.deal_type == 1:
            self.total_weights = self.weights_type1
        elif self.deal_type == 2 or self.deal_type == 4:
            self.total_weights = self.weights_type3
    def cal_custom(self, custom):
        if custom == 'AA':
            return 100
        elif custom == 'A':
            return 70
        elif custom == 'B':
            return 50
        else:
            return 50

    def cal_tax_credit(self, tax_credit):
        if tax_credit == 'A':
            return 100
        elif tax_credit == 'B':
            return 70
        elif tax_credit == 'C':
            return 30
        elif tax_credit == 'D':
            return 0
        else:
            return 50

    def cal_ADMINISTRATION(self, total_num, three_years_num, start_from_now):
        if total_num == 'N/A':
            total_num = 0
        if three_years_num == 'N/A':
            three_years_num = 0
        if total_num == 0 and start_from_now >= 3:
            return 100
        elif total_num == 0 and start_from_now < 3:
            return 70
        elif total_num > 0 and three_years_num == 0:
            return 80
        elif three_years_num > 0 and three_years_num <= 3:
            return 65
        elif three_years_num > 3:
            return 50

    def cal_CRIMINAL(self, custom_num, non_custom_num):
        if custom_num == 'N/A':
            custom_num = 0
        if non_custom_num == 'N/A':
            non_custom_num = 0
        return 100 - 50 * non_custom_num - 100 * custom_num

    def cal_LOST_CREDIT(self, total, lost_1st, lost_2nd, lost_3rd, start_from_now):
        if total == 'N/A':
            total = 0
        if lost_1st == 'N/A':
            lost_1st = 0
        if lost_2nd == 'N/A':
            lost_2nd = 0
        if lost_3rd == 'N/A':
            lost_3rd = 0
        if total == 0 and start_from_now >= 3:
            return 100
        elif total == 0 and start_from_now < 3:
            return 80
        else:
            return 100 - 50 * lost_1st - 35 * lost_2nd - 25 * lost_3rd

    def cal_EXCUTED(self, total, excuted_1st, excuted_2nd, excuted_3rd, start_from_now):
        if total == 'N/A':
            total = 0
        if excuted_1st == 'N/A':
            excuted_1st = 0
        if excuted_2nd == 'N/A':
            excuted_2nd = 0
        if excuted_3rd == 'N/A':
            excuted_3rd = 0
        if total == 0 and start_from_now >= 3:
            return 100
        elif total == 0 and start_from_now < 3:
            return 90
        else:
            return 100 - 20 * excuted_1st - 15 * excuted_2nd - 10 * excuted_3rd

    def cal_NOT_OPERATION(self, total, three_years_num, start_from_now):
        if total == 'N/A':
            total = 0
        if three_years_num == 'N/A':
            three_years_num = 0
        if total == 0 and start_from_now >= 5:
            return 100
        elif total == 0 and start_from_now < 5:
            return 80
        elif three_years_num == 1:
            return 60
        elif three_years_num == 2:
            return 40
        elif three_years_num >= 3:
            return 0
        else:
            return 80

    def cal_SOCIAL_SECURE(self, num):
        if num == 'N/A':
            num = 0
        if num >= 500:
            return 100
        elif num < 500 and num >= 4:
            return 80
        elif num < 4 and num > 0:
            return 30
        else:
            return 0

    def cal_TOTAL_SCORE(self, custom, criminal_in_five, total_score):
        total_res = 0
        if custom == 'N/A' or custom == 'C':
            total_res = 0
        elif criminal_in_five >= 1:
            total_res = 0
        else:
            total_res = total_score
        return total_res

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

    def logic_layer(self):
        sql_text = f'''select CREDIT_CODE, ENTRY_ID, D_DATE, I_E_FLAG, DEAL_TYPE,
        ADMINISTRATION, ADMINISTRATION_3_YEARS, 
        CRIMINAL_CUSTOM, CRIMINAL_NON_CUSTOM, 
        CUSTOM, 
        EXCUTED_TOTAL, EXCUTED_1ST, EXCUTED_2ND, EXCUTED_3RD, 
        LOST_CREDIT_TOTAL, LOST_CREDIT_1ST, LOST_CREDIT_2ND, LOST_CREDIT_3RD, 
        NOT_OPERATION, NOT_OPERATION_3_YEARS, 
        SOCIAL_SECURE, 
        START_FROM_NOW, 
        TAX_CREDIT 
        FROM DW_CUS_RC.BD_RISK_CROSS_TRADE_RESULT_CREDIT_SCORE_TBL_CLEAN 
        WHERE DEAL_TYPE = {self.deal_type}
        AND CAL_TYPE = 1
                    '''
        SupplyChain_df = Read_Oracle().read_oracle(sql=sql_text, database='dbalarm')
        print(SupplyChain_df)

        sql_text2 = f'''select CREDIT_CODE, ENTRY_ID, D_DATE, I_E_FLAG, DEAL_TYPE
                FROM DW_CUS_RC.BD_RISK_CROSS_TRADE_RESULT_CREDIT_SCORE_TBL_CLEAN 
                WHERE DEAL_TYPE = {self.deal_type}
                AND CAL_TYPE = 0
                            '''
        zero_df = Read_Oracle().read_oracle(sql=sql_text2, database='dbalarm')

        if SupplyChain_df is not None and len(SupplyChain_df) != 0:
            res_df = pd.DataFrame()
            res_df['CUSTOM_SCORE'] = SupplyChain_df.apply(lambda x: self.cal_custom(x['CUSTOM']), axis=1)
            res_df['TAX_CREDIT_SCORE'] = SupplyChain_df.apply(lambda x: self.cal_tax_credit(x['TAX_CREDIT']), axis=1)
            res_df['ADMINISTRATION_SCORE'] = SupplyChain_df.apply(lambda x: self.cal_ADMINISTRATION(x['ADMINISTRATION'], x['ADMINISTRATION_3_YEARS'], x['START_FROM_NOW']), axis=1)
            res_df['CRIMINAL_SCORE'] = SupplyChain_df.apply(lambda x: self.cal_CRIMINAL(x['CRIMINAL_CUSTOM'], x['CRIMINAL_NON_CUSTOM']), axis=1)
            res_df['LOST_CREDIT_SCORE'] = SupplyChain_df.apply(lambda x: self.cal_LOST_CREDIT(x['LOST_CREDIT_TOTAL'], x['LOST_CREDIT_1ST'], x['LOST_CREDIT_2ND'], x['LOST_CREDIT_3RD'], x['START_FROM_NOW']), axis=1)
            res_df['EXCUTED_SCORE'] = SupplyChain_df.apply(lambda x: self.cal_EXCUTED(x['EXCUTED_TOTAL'], x['EXCUTED_1ST'], x['EXCUTED_2ND'], x['EXCUTED_3RD'], x['START_FROM_NOW']), axis=1)
            res_df['NOT_OPERATION_SCORE'] = SupplyChain_df.apply(lambda x: self.cal_NOT_OPERATION(x['NOT_OPERATION'], x['NOT_OPERATION_3_YEARS'], x['START_FROM_NOW']), axis=1)
            res_df['SOCIAL_SECURE_SCORE'] = SupplyChain_df.apply(lambda x: self.cal_SOCIAL_SECURE(x['SOCIAL_SECURE']), axis=1)
            print("------------------------")
            print(res_df)
            res_df_num = len(res_df)
            weights_arr1 = self.copyList(self.weights[0: 2], res_df_num)
            res_df['CREDIT_RISK_SCORE'] = self.get_weightScore_totalScore(res_df[['CUSTOM_SCORE', 'TAX_CREDIT_SCORE']], weights_arr1, self.columns[0: 2])
            weights_arr2 = self.copyList(self.weights[2: 6], res_df_num)
            res_df['ENTERPRISE_RISK_SCORE'] = self.get_weightScore_totalScore(res_df[['ADMINISTRATION_SCORE', 'CRIMINAL_SCORE', 'LOST_CREDIT_SCORE', 'EXCUTED_SCORE']], weights_arr2, self.columns[2: 6])
            weights_arr3 = self.copyList(self.weights[6: ], res_df_num)
            res_df['OTHER_RISK_SCORE'] = self.get_weightScore_totalScore(res_df[['NOT_OPERATION_SCORE', 'SOCIAL_SECURE_SCORE']], weights_arr3, self.columns[6: ])

            total_weights = self.copyList(self.total_weights, res_df_num)
            res_df['TOTAL_SCORE'] = self.get_weightScore_totalScore(res_df[['CREDIT_RISK_SCORE', 'ENTERPRISE_RISK_SCORE', 'OTHER_RISK_SCORE']], total_weights, self.total_columns)

            print("*******************************")
            if res_df is not None and len(res_df) != 0:
                for i in range(0, len(self.columns)):
                    res_df[self.columns[i]] = self.weights[i]
                for j in range(0, len(self.total_columns)):
                    res_df[self.total_columns[j]] = self.total_weights[j]
                res_df['ID'] = 0
                res_df['CREDIT_CODE'] = SupplyChain_df['CREDIT_CODE']
                res_df['I_E_FLAG'] = SupplyChain_df['I_E_FLAG']
                res_df['D_DATE'] = SupplyChain_df['D_DATE']
                res_df['ENTRY_ID'] = SupplyChain_df['ENTRY_ID']
                res_df['DEAL_TYPE'] = SupplyChain_df['DEAL_TYPE']
                print(res_df)
                # 写入数据库
                Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_RESULT_CREDIT_SCORE_TBL', res_df, org_code=None, alarm=None)

            if zero_df is not None and len(zero_df) != 0:
                zero_df['TOTAL_SCORE'] = 0
                zero_df['ID'] = 0
                Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_RESULT_CREDIT_SCORE_TBL', zero_df, org_code=None, alarm=None)

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
        child_task_id = 'SupplyChain_Logic'
    else:
        child_task_id = sys.argv[1]
    # org_code, param_json, base_time = read_log_table(child_task_id)
    for i in [1, 2, 4]:
        SupplyChain_Logic(None, None, child_task_id, i).run_logic_layer()