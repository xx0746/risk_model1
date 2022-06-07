import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"F:/bdrisk_model/risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class ScoreSu(object):
    def __init__(self, org_code, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)
        # 参数读取
        self.database = "dbalarm" if params_global.is_test else "dbdw"

    def score_supervisor(self):
        model_code = 'SUPERVISOR'
        # 读取企业库存模块的结果表数据
        in_result = Read_Oracle().read_oracle(sql=f""" select a.BIZOP_ETPS_SCCD,a.BIZOP_ETPS_NO,a.THREE_FLOOR,a.FOURTH_FLOOR,COUNT(DISTINCT DEAL_WITH_NO) AS EVENT_NUMBER FROM DEAL_WITH a 
        WHERE a.SECOND_FLOOR = 'QLCWLJKYJ' AND a.STATUS in ('48','49')
        GROUP BY a.BIZOP_ETPS_SCCD,a.BIZOP_ETPS_NO,a.THREE_FLOOR,a.FOURTH_FLOOR""",database='dblgsa')

        if len(in_result) != 0:
            in_result.columns = ['ORG_CODE', 'HG_ORG_ID', 'CHILD_MODEL_CODE', 'LABEL', 'EVENT_NUMBER']
            total_event = in_result['EVENT_NUMBER'].sum()
        else:
            total_event = 0
            in_result = pd.DataFrame(columns = ['ORG_CODE', 'HG_ORG_ID', 'SCORE'])

        if total_event != 0:
            # 各个扣分原因的直接扣分
            in_result['SCORE'] = -in_result['EVENT_NUMBER'] * 0.1

            # 企业的追加扣分
            org_event = pd.DataFrame(in_result.groupby(['ORG_CODE', 'HG_ORG_ID'])['EVENT_NUMBER'].sum())
            org_event = org_event.stack()
            org_event = org_event.reset_index()
            org_event.columns = ['ORG_CODE', 'HG_ORG_ID', 'NAME', 'EVENT_NUMBER']
            org_event = org_event[['ORG_CODE', 'HG_ORG_ID', 'EVENT_NUMBER']].copy()
            org_event['EVENT_PER'] = org_event['EVENT_NUMBER']/total_event
            org_event['SCORE'] = org_event['EVENT_PER'].apply(lambda x: round(-(x - 0.1)*100,2) if x>0.1 else 0)
            org_result = org_event[org_event['SCORE']<0].copy()
            org_result = org_result[['ORG_CODE', 'HG_ORG_ID', 'SCORE']].copy()
            org_result['CHILD_MODEL_CODE'] = 'ALL'
            org_result['LABEL'] = '追加扣分'
            org_result['EVENT_NUMBER'] = 1
            # 合并结果
            df_result = pd.concat([in_result,org_result])
            df_result['MODEL_CODE'] = model_code
        else:
            df_result = pd.DataFrame(columns = ['ORG_CODE', 'HG_ORG_ID', 'SCORE'])


        business_time = (datetime.date.today().replace(day=1) - datetime.timedelta(1)).replace(day=1)

        if len(df_result) != 0:
            df_result['BUSINESS_TIME'] = business_time
            df_result['BUSINESS_TIME'] = df_result['BUSINESS_TIME'].astype('datetime64')
            df_result = df_result.reset_index(drop=True)
            df_result = df_result.reset_index().rename(columns={'index': 'ID'})
            # 写入
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_DTL', df_result, org_code = None, alarm = [model_code,''])

        sql = f"""
        select DISTINCT ORG_CODE,TRADE_CODE,LABEL_CODE from BD_RISK_CORP_INFO_BASIC WHERE ISCURRENT = 1
        """
        corp_list = Read_Oracle().read_oracle(sql=sql, database="dbalarm")
        if corp_list.shape[0] == 0:
            print('ERROR: 未获取到任何企业信息')
            return
        corp_list.columns = ['ORG_CODE','HG_ORG_ID','LABEL_CODE']
        corp_list['ORG_TYPE'] = corp_list['LABEL_CODE'].apply(lambda x: x.split(sep=',')[0])
        corp_list = corp_list[['ORG_CODE','HG_ORG_ID','ORG_TYPE']].copy()

        if len(df_result) != 0:
            df_score = df_result[['ORG_CODE','HG_ORG_ID','SCORE']].copy()
            df_score = pd.DataFrame(df_score.groupby(['ORG_CODE', 'HG_ORG_ID'])['SCORE'].sum())
            df_score = df_score.stack()
            df_score = df_score.reset_index()
            df_score.columns = ['ORG_CODE', 'HG_ORG_ID', 'NAME', 'SCORE']
            df_score = df_score[['ORG_CODE', 'SCORE']].copy()
        else:
            df_score = pd.DataFrame(columns = ['ORG_CODE', 'SCORE'])
        df_score = pd.merge(corp_list,df_score,how='left',on=['ORG_CODE'])
        df_score['SCORE'] = df_score['SCORE'].fillna(0)
        df_score['SCORE'] = df_score['SCORE'].apply(lambda x: round((100 + x),2))
        df_score['MODEL_CODE'] = model_code
        df_score['BUSINESS_TIME'] = business_time
        df_score['BUSINESS_TIME'] = df_score['BUSINESS_TIME'].astype('datetime64')
        df_score = df_score.reset_index(drop=True)
        df_score = df_score.reset_index().rename(columns={'index': 'ID'})
        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE', df_score, org_code = None, alarm = [model_code,''])

        df_cm = df_score[['ORG_CODE', 'HG_ORG_ID', 'SCORE','MODEL_CODE','BUSINESS_TIME']].copy()
        df_cm['CHILD_MODEL_CODE'] = model_code
        df_cm = df_cm.reset_index(drop=True)
        df_cm = df_cm.reset_index().rename(columns={'index': 'ID'})
        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_CM', df_cm, org_code = None, alarm = [model_code,''])

    def run_score_supervisor(self):
        try:
            self.score_supervisor()
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
    score_in = ScoreSu(org_code = org_code, base_time = base_time, child_task_id = child_task_id)
    score_in.run_score_supervisor()