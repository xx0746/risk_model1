import sys, os
from os import path
if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"F:/bdrisk_model/risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *

class ScoreFn(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)
        self.df_result = None
        self.df_watcher = None
        # 参数读取
        self.org_code = org_code
        self.score_weight = json.loads(params)['score_weight']
        self.database = "dbalarm" if params_global.is_test else "dbdw"
        
    def score_fn(self):  
        model_code = 'FINANCE'

        # 读数
        sql=f"""
        select ORG_CODE,RISK_LABEL,sum(SCORE),COUNT(*) from {TableList.BD_RISK_RESULT_FINANCE_FN1.value} WHERE SCORE < 0 AND ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' GROUP BY ORG_CODE,RISK_LABEL
        """
        fn1_result = Read_Oracle().read_oracle(sql=sql,database=self.database)

        if len(fn1_result) != 0:
            fn1_result.columns = ['ORG_CODE','LABEL','SCORE','EVENT_NUMBER']
            fn1_result['CHILD_MODEL_CODE'] = 'FN1'
            fn1_score = fn1_result['SCORE'].sum()
        else:
            fn1_score = 0

        sql=f"""
        select ORG_CODE,RISK_LABEL,sum(SCORE),COUNT(*) from {TableList.BD_RISK_RESULT_FINANCE_FN2.value} WHERE SCORE < 0 AND ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' GROUP BY ORG_CODE,RISK_LABEL \
        """
        fn2_result = Read_Oracle().read_oracle(sql=sql, database=self.database)
        if len(fn2_result) != 0:
            fn2_result.columns = ['ORG_CODE','LABEL','SCORE','EVENT_NUMBER']
            fn2_result['CHILD_MODEL_CODE'] = 'FN2'
            fn2_score = fn2_result['SCORE'].sum()
        else:
            fn2_score = 0

        sql = f"""
        select TRADE_CODE,LABEL_CODE from BD_RISK_CORP_INFO_BASIC WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}'
            """
        corp_list = Read_Oracle().read_oracle(sql=sql, database=self.database)
        if len(corp_list) == 0:
            hg_id = ''
            org_type = ''
        else:
            hg_id = corp_list.loc[0,'TRADE_CODE']
            org_type = corp_list.loc[0,'LABEL_CODE']
            org_type = org_type.split(sep=',')[0]

        df_result = pd.concat([fn1_result,fn2_result])
        business_time = (datetime.date.today().replace(day=1) - datetime.timedelta(1)).replace(day=1)

        total_score = (100+fn1_score)*self.score_weight[0] + (100+fn2_score)*self.score_weight[1]
        total_score = total_score / (self.score_weight[0] + self.score_weight[1])

        if len(df_result) != 0:
            df_result = df_result[df_result['SCORE']<0].copy()
            if len(df_result) != 0:
                df_result['MODEL_CODE'] = model_code
                df_result['HG_ORG_ID'] = hg_id
                df_result['BUSINESS_TIME'] = business_time
                df_result['BUSINESS_TIME'] = df_result['BUSINESS_TIME'].astype('datetime64')
                df_result = df_result.reset_index(drop=True)
                df_result = df_result.reset_index().rename(columns={'index': 'ID'})
                Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_DTL', df_result, org_code = self.org_code,alarm = [model_code,''])
        
        data = {'SCORE':[100+fn1_score, 100+fn2_score],'CHILD_MODEL_CODE':['FN1','FN2'],'ID':[0,1]}
        df_cm = pd.DataFrame(data,index=range(2))
        df_cm['ORG_CODE'] = self.org_code
        df_cm['MODEL_CODE'] = model_code
        df_cm['HG_ORG_ID'] = hg_id
        df_cm['BUSINESS_TIME'] = business_time
        df_cm['BUSINESS_TIME'] = df_cm['BUSINESS_TIME'].astype('datetime64')
        
        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_CM', df_cm, org_code = self.org_code,alarm = [model_code,''])

        data2 = {'SCORE':total_score,'ID':0}
        df_score = pd.DataFrame(data2,index=range(1))
        df_score['ORG_CODE'] = self.org_code
        df_score['HG_ORG_ID'] = hg_id
        df_score['ORG_TYPE'] = org_type
        df_score['MODEL_CODE'] = 'FINANCE'
        df_score['BUSINESS_TIME'] = business_time
        df_score['BUSINESS_TIME'] = df_score['BUSINESS_TIME'].astype('datetime64')
        
        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE', df_score, org_code = self.org_code, alarm = [model_code,''])

    def run_score_fn(self):
        try:
            self.score_fn()
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
    if params_global.is_test:
        child_task_id = sys.argv[1]
    else:
        child_task_id = sys.argv[1]
    org_code, param_json, basetime = read_log_table(child_task_id)
    # org_code = '91310000132612172J'
    # param_json = '{"score_weight":[0.5,0.5]}'
    # basetime = ''
    score_fn = ScoreFn(org_code, params=param_json, base_time=basetime, child_task_id=child_task_id)
    score_fn.run_score_fn()