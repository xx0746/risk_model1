import sys, os
from os import path
if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"F:/bdrisk_model/risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_RESULT_PRODUCTION_PD1, _name_BD_RISK_RESULT_PRODUCTION_PD2, _name_BD_RISK_RESULT_PRODUCTION_PD3, _name_BD_RISK_RESULT_PRODUCTION_PD4


class ScorePd(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status=None)
        self.df_result = None
        self.df_watcher = None
        # 参数读取
        self.org_code = org_code
        self.weights = json.loads(params)['weights']
        self.database = "dbalarm" if params_global.is_test else "dbdw"

    def score_production(self):
        model_code = 'PRODUCTION'

        # 读取企业库存模块的结果表数据
        PD1_RESULT = Read_Oracle().read_oracle(sql= """ select ORG_CODE,RISK_LABEL,sum(SCORE),COUNT(*) from {} where org_code = '{}' and iscurrent = 1 and SCORE < 0 GROUP BY ORG_CODE,RISK_LABEL""".format(_name_BD_RISK_RESULT_PRODUCTION_PD1, self.org_code), database=self.database)
        PD2_RESULT = Read_Oracle().read_oracle(sql= """ select ORG_CODE,RISK_LABEL,sum(SCORE),COUNT(*) from {} where org_code = '{}' and iscurrent = 1 and SCORE < 0 GROUP BY ORG_CODE,RISK_LABEL """.format(_name_BD_RISK_RESULT_PRODUCTION_PD2, self.org_code), database=self.database)
        PD3_RESULT = Read_Oracle().read_oracle(sql= """ select ORG_CODE,RISK_LABEL,sum(SCORE),COUNT(*) from {} where org_code = '{}' and iscurrent = 1 and SCORE < 0 GROUP BY ORG_CODE,RISK_LABEL """.format(_name_BD_RISK_RESULT_PRODUCTION_PD3, self.org_code), database=self.database)
        PD4_RESULT = Read_Oracle().read_oracle(sql= """ select ORG_CODE,RISK_LABEL,sum(SCORE),COUNT(*) from {} where org_code = '{}' and iscurrent = 1 and SCORE < 0 GROUP BY ORG_CODE,RISK_LABEL """.format(_name_BD_RISK_RESULT_PRODUCTION_PD4, self.org_code), database=self.database)
#         RESULT_LIST = [PD1_RESULT, PD2_RESULT, PD3_RESULT, PD4_RESULT]

        if len(PD1_RESULT) == 0:   
            pd1_score = 0
        else:
            PD1_RESULT.columns = ['ORG_CODE','LABEL','SCORE','EVENT_NUMBER']
            PD1_RESULT['CHILD_MODEL_CODE'] = 'PD1'
            pd1_score = PD1_RESULT['SCORE'].sum()

        if len(PD2_RESULT) == 0:        
            pd2_score = 0
        else:
            PD2_RESULT.columns = ['ORG_CODE','LABEL','SCORE','EVENT_NUMBER']
            PD2_RESULT['CHILD_MODEL_CODE'] = 'PD2'
            pd2_score = PD2_RESULT['SCORE'].sum()
        
        if len(PD3_RESULT) == 0:
            pd3_score = 0
        else:
            PD3_RESULT.columns = ['ORG_CODE','LABEL','SCORE','EVENT_NUMBER']
            PD3_RESULT['CHILD_MODEL_CODE'] = 'PD3'
            pd3_score = PD3_RESULT['SCORE'].sum()
        
        if len(PD4_RESULT) == 0:
            pd4_score = 0
        else:
            PD4_RESULT.columns = ['ORG_CODE','LABEL','SCORE','EVENT_NUMBER']
            PD4_RESULT['CHILD_MODEL_CODE'] = 'PD4'
            pd4_score = PD4_RESULT['SCORE'].sum()

        sql=f"""\
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
    
        df_result = pd.concat([PD1_RESULT,PD2_RESULT,PD3_RESULT,PD4_RESULT])
        business_time = (datetime.date.today().replace(day=1) - datetime.timedelta(1)).replace(day=1)
        
        total_score = (100+pd1_score)*self.weights[0] + (100+pd2_score)*self.weights[1] + (100+pd3_score)*self.weights[2] + (100+pd4_score)*self.weights[3]
        total_score = total_score / (self.weights[0] + self.weights[1] + self.weights[2] + self.weights[3])

        if df_result.shape[0] != 0:
            df_result['MODEL_CODE'] = model_code
            df_result['HG_ORG_ID'] = hg_id
            df_result['BUSINESS_TIME'] = business_time
            df_result['BUSINESS_TIME'] = df_result['BUSINESS_TIME'].astype('datetime64')
            df_result = df_result.reset_index(drop=True)
            df_result = df_result.reset_index().rename(columns={'index': 'ID'})
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_DTL', df_result, org_code = self.org_code,alarm = [model_code,''])
        
        data = {'SCORE':[100+pd1_score,100+pd2_score,100+pd3_score,100+pd4_score],'CHILD_MODEL_CODE':['PD1','PD2','PD3','PD4'],'ID':[0,1,2,3]}
        df_cm = pd.DataFrame(data,index=range(4))
        df_cm['ORG_CODE'] = self.org_code
        df_cm['MODEL_CODE'] = model_code
        df_cm['HG_ORG_ID'] = hg_id
        df_cm['BUSINESS_TIME'] = business_time
        df_cm['BUSINESS_TIME'] = df_cm['BUSINESS_TIME'].astype('datetime64')
        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_CM', df_cm, org_code = self.org_code, alarm = [model_code,''])

        data2 = {'SCORE':total_score,'ID':0}
        df_score = pd.DataFrame(data2,index=range(1))
        df_score['ORG_CODE'] = self.org_code
        df_score['HG_ORG_ID'] = hg_id
        df_score['ORG_TYPE'] = org_type
        df_score['MODEL_CODE'] = model_code
        df_score['BUSINESS_TIME'] = business_time
        df_score['BUSINESS_TIME'] = df_score['BUSINESS_TIME'].astype('datetime64')
        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE', df_score, org_code = self.org_code, alarm = [model_code,''])

    def run_score_production(self):
        try:
            self.score_production()
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
    org_code, params, base_time = read_log_table(child_task_id)
    # org_code = "91310000132612172J"
    score_pd = ScorePd(org_code, params, base_time, child_task_id)
    score_pd.run_score_production()