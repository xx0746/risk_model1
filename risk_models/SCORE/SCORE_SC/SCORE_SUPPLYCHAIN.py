import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"F:/bdrisk_model/risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_RESULT_SUPPLYCHAIN_SC1


class ScoreSc(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)
        self.df_result = None
        self.df_watcher = None
        # 参数读取
        self.org_code = org_code
        self.database = "dbalarm" if params_global.is_test else "dbdw"

    def score_supplychain(self):
        model_code = 'SUPPLY_CHAIN'
        check_date = (datetime.datetime.now()+datetime.timedelta(days=-365-367)).strftime('%Y%m%d')
        cr_result = Read_Oracle().read_oracle(sql=f""" SELECT CREDIT_CODE_OPERATION,AVG(TOTAL_SCORE) FROM 
        (SELECT I_E_FLAG,CREDIT_CODE_OPERATION,AVG(TOTAL) AS TOTAL_SCORE FROM BD_RISK_SUPPLY_CHAIN_SCORE 
        WHERE ISCURRENT = 1 GROUP BY I_E_FLAG,CREDIT_CODE_OPERATION)
        GROUP BY CREDIT_CODE_OPERATION""", database='dbalarm')
#         cr_result = Read_Oracle().read_oracle(sql=f""" SELECT CREDIT_CODE_OPERATION,AVG(TOTAL) FROM 
# BD_RISK_CROSS_TRADE_RESULT_SUPPLY_CHAIN_SCORE_TBL 
# WHERE ISCURRENT = 1 
# GROUP BY CREDIT_CODE_OPERATION""", database='dbalarm')

        if len(cr_result) != 0:
            cr_result.columns = ['ORG_CODE','SCORE']
        else:
            cr_result = pd.DataFrame(columns = ['ORG_CODE','SCORE'])

        sql=f"""
        select DISTINCT ORG_CODE,TRADE_CODE,LABEL_CODE from BD_RISK_CORP_INFO_BASIC WHERE ISCURRENT = 1
        """
        corp_list = Read_Oracle().read_oracle(sql=sql, database=self.database)
        if corp_list.shape[0] == 0:
            print('ERROR: 未获取到任何企业信息')
            return
        corp_list.columns = ['ORG_CODE','HG_ORG_ID','LABEL_CODE']
        corp_list['ORG_TYPE'] = corp_list['LABEL_CODE'].apply(lambda x: x.split(sep=',')[0])
        corp_list = corp_list[['ORG_CODE','HG_ORG_ID','ORG_TYPE']].copy()

        business_time = (datetime.date.today().replace(day=1) - datetime.timedelta(1)).replace(day=1)

        df_score = cr_result[['ORG_CODE','SCORE']].copy()
        df_score = pd.merge(corp_list,df_score,how='left',on=['ORG_CODE'])
        df_score['SCORE'] = df_score['SCORE'].fillna(100)
        df_score['SCORE'] = df_score['SCORE'].astype(float)
        df_score['SCORE'] = df_score['SCORE'].apply(lambda x: round((x),2))
        df_score['MODEL_CODE'] = model_code
        df_score['BUSINESS_TIME'] = business_time
        df_score = df_score.reset_index(drop=True)
        df_score = df_score.reset_index().rename(columns={'index': 'ID'})
        df_score['BUSINESS_TIME'] = df_score['BUSINESS_TIME'].astype('datetime64')

        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE', df_score, org_code = None, alarm = [model_code,''])

        df_score = df_score[['ORG_CODE','HG_ORG_ID','MODEL_CODE','BUSINESS_TIME','SCORE']].copy()
        df_score['CHILD_MODEL_CODE'] = model_code
        df_score = df_score.reset_index(drop=True)
        df_score = df_score.reset_index().rename(columns={'index': 'ID'})
        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_CM', df_score, org_code = None, alarm = [model_code,''])

        df_score = df_score[df_score['SCORE']<100].copy()
        if df_score.shape[0] != 0:
            df_score['SCORE'] = df_score['SCORE'] - 100
            df_score['SCORE'] = df_score['SCORE'].astype(float)
            df_score['SCORE'] = df_score['SCORE'].apply(lambda x: round((x),2))
            df_score = df_score[['ORG_CODE','HG_ORG_ID','MODEL_CODE','CHILD_MODEL_CODE','BUSINESS_TIME','SCORE']].copy()
            df_score['LABEL'] = '供应链模型扣分'
            df_score['EVENT_NUMBER'] = 1
            df_score = df_score.reset_index(drop=True)
            df_score = df_score.reset_index().rename(columns={'index': 'ID'})
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_DTL', df_score, org_code = None,alarm = [model_code,''])

    def run_score_supplychain(self):
        try:
            self.score_supplychain()
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
    # org_code, params, base_time, child_task_id
    # org_code = None
    score_sc = ScoreSc(org_code, params, base_time, child_task_id)
    score_sc.run_score_supplychain()