import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk-model\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_RESULT_TRADE_TD2, _name_BD_RISK_RESULT_TRADE_TD1


class ScoreTd(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)
        self.df_result = None
        self.df_watcher = None
        # 参数读取
        self.org_code = org_code
        self.enddt = (dt_method.strptime(base_time, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=62)) \
            .strftime('%Y-%m-%d')
        self.database = "dbods" if params_global.is_test else "dbdm"

    def score_trade(self):
        # 读取企业库存模块的结果表数据
        td2_result = Read_Oracle().read_oracle(
            sql=f""" select sum(SCORE) from {_name_BD_RISK_RESULT_TRADE_TD2} where org_code = '{self.org_code}' 
            and iscurrent = 1 and CUSTOMER_CODE = 'FTA_LG' and BIZ_DATE >= date'{self.enddt}' """, database=self.database)
        td1_result = Read_Oracle().read_oracle(
            sql=f""" select sum(SCORE) from {_name_BD_RISK_RESULT_TRADE_TD1} where org_code = '{self.org_code}' and iscurrent = 1 and CUSTOMER_CODE = 'FTA_LG' """,
            database=self.database)

        x = [td1_result.iloc[0,0], td2_result.iloc[0,0]]
    
        total_score = 0
        for i in range(len(x)):
            if pd.isnull(x[i]):
                total_score = total_score + 0
            elif x[i] < -100 :
                total_score = total_score + (100 - 100 )
            elif x[i] > 0 :
                total_score = total_score + (100 + 0 )
            else:
                total_score = total_score + (100 + x[i])

        if total_score < 20:
            total_score = 20

#         if td1_result is None:
#             TD1_SCORE = 0
#         else:
#             TD1_SCORE = td1_result.groupby('ORG_CODE')['SCORE'].sum()[0]
#         if td2_result is None:
#             TD2_SCORE = 0
#         else:
#             TD2_SCORE = td2_result.groupby('ORG_CODE')['SCORE'].sum()[0]

#         total_score = 100 + TD2_SCORE + TD1_SCORE
#         if total_score < 0:
#             total_score = 0
        # print(total_score)
        # 整理分数表并写入数据库
        df = pd.DataFrame(data=[[0, self.org_code, 'TRADE', total_score]],
                          columns=['ID', 'ORG_CODE', 'MODEL_CODE', 'SCORE'])
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        df['CUSTOMER_CODE'] = 'FTA_LG'
        self.df_result = df
        self.df_watcher = df
        self.df_watcher["td1_result"] = td1_result
        self.df_watcher["td2_result"] = td2_result

    def write_to_oracle(self, df):
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_CORP_SCORE_DISPLAY.value}', df,
                                        org_code=self.org_code, alarm=['TRADE', ''])

    def get_df_result(self):
        return self.df_watcher


    def run_score_trade(self):
        try:
            self.score_trade()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    if params_global.is_test:
        child_task_id = 'childtaskidscoretd'
    else:
        child_task_id = sys.argv[1]
    org_code, params, base_time = read_log_table(child_task_id)
    score_td = ScoreTd(child_task_id, org_code, params, base_time)
    score_td.run_score_trade()
    score_td.write_to_oracle(score_td.df_result)
