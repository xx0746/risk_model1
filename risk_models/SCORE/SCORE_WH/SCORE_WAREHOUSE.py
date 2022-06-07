import sys, os
from os import path
if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk-model\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *

class ScoreWh(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)
        self.df_result = None
        self.df_watcher = None
        # 参数读取
        self.org_code = org_code
        self.score_weight = json.loads(params)['score_weight']
        self.database = "dbods" if params_global.is_test else "dbdm"
        
    def score_wh(self):  
        # 读数
        sql=f"""\
        select sum(SCORE) from {TableList.BD_RISK_RESULT_WAREHOUSE_WH1.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' and CUSTOMER_CODE = 'FTA_LG' \
        """
        wh1_result = Read_Oracle().read_oracle(sql=sql, database=self.database)

        sql=f"""\
        select sum(SCORE) from {TableList.BD_RISK_RESULT_WAREHOUSE_WH2.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' and CUSTOMER_CODE = 'FTA_LG' \
        """
        wh2_result = Read_Oracle().read_oracle(sql=sql,database=self.database)

        x = [wh1_result.iloc[0,0],wh2_result.iloc[0,0]]

        total_score = 0
        for i in range(len(x)):
            if pd.isnull(x[i]):
                total_score = total_score + 0
            elif x[i] < -100 :
                total_score = total_score + (100 - 100 )*self.score_weight[i]
            elif x[i] > 0 :
                total_score = total_score + (100 + 0 )*self.score_weight[i]
            else:
                total_score = total_score + (100 + x[i])*self.score_weight[i]
                
        if total_score < 20:
            total_score = 20
                
        total_score = round(total_score,2)
        df_result = pd.DataFrame()
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result = df_result.append([{'ID':0,'ORG_CODE':self.org_code,'CHECK_TIME':now}],ignore_index = True)
        df_result['CHECK_TIME'] = pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        df_result['SCORE'] = total_score
        df_result['MODEL_CODE'] = 'WAREHOUSE'
        df_result['CUSTOMER_CODE'] = 'FTA_LG'
        self.df_result = df_result
        self.df_watcher = df_result
        self.df_watcher["wh1_result"] = wh1_result
        self.df_watcher["wh2_result"] = wh2_result

    def write_to_oracle(self, df):
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_CORP_SCORE_DISPLAY.value}', df,
                                    org_code=self.org_code, alarm=['WAREHOUSE', ''])

    def get_df_result(self):
        return self.df_watcher

    def run_score_wh(self):
        try:
            self.score_wh()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    if params_global.is_test:
        child_task_id = 'childtaskidscorefn'
    else:
        child_task_id = sys.argv[1]
    org_code, param_json, basetime = read_log_table(child_task_id)
    score_wh = ScoreWh(org_code, params=param_json, base_time=basetime, child_task_id=child_task_id)
    score_wh.run_score_wh()
    score_wh.write_to_oracle(score_wh.df_result)