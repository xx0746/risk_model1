import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"F:/bdrisk_model/risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_RESULT_TRACK_TR1


class ScoreTr(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)
        # 参数读取
        self.database = "dbalarm" if params_global.is_test else "dbdw"

    def score_track(self):
        model_code = 'LOGISTIC'
        tr_result = Read_Oracle().read_oracle(sql=f""" select ORG_CODE,TRACK_FLAG,TIME_FLAG FROM BD_RISK_RESULT_TRACK_TR1 WHERE TRACK_FLAG in (0,1) AND TIME_FLAG in (0,1) """,database='dbalarm')

        if tr_result is not None and len(tr_result) != 0:
            tr_result.columns = ['ORG_CODE', 'TRACK_FLAG','TIME_FLAG']
            tr_result['TRACK_FLAG'] = tr_result['TRACK_FLAG'].astype(float)
            tr_result['TIME_FLAG'] = tr_result['TIME_FLAG'].astype(float)
            total_event = tr_result['TRACK_FLAG'].count()
        else:
            total_event = 0

        if total_event == 0:
            print('上个自然月无物流模型数据')
            return

        tr_err_result = tr_result[tr_result['TRACK_FLAG']==1].copy()
        tr_err_event = tr_err_result['TRACK_FLAG'].count()

        time_err_result = tr_result[tr_result['TIME_FLAG']==1].copy()
        time_err_event = time_err_result['TIME_FLAG'].count()

        tr_err_rate = tr_err_event/total_event
        time_err_rate = time_err_event/total_event

        # 按企业汇总
        tr_result['EVENET'] = 1
        tr_result['EVENET'] = tr_result['EVENET'].astype(float)

        org_event = pd.DataFrame(tr_result.groupby(['ORG_CODE'])['EVENET'].sum())
        org_event = org_event.stack()
        org_event = org_event.reset_index()
        org_event.columns = ['ORG_CODE', 'NAME', 'EVENT']
        org_event = org_event[['ORG_CODE','EVENT']].copy()

        org_tr_event = pd.DataFrame(tr_result.groupby(['ORG_CODE'])['TRACK_FLAG'].sum())
        org_tr_event = org_tr_event.stack()
        org_tr_event = org_tr_event.reset_index()
        org_tr_event.columns = ['ORG_CODE', 'NAME', 'TRACK']
        org_tr_event = org_tr_event[['ORG_CODE','TRACK']].copy()

        org_time_event = pd.DataFrame(tr_result.groupby(['ORG_CODE'])['TIME_FLAG'].sum())
        org_time_event = org_time_event.stack()
        org_time_event = org_time_event.reset_index()
        org_time_event.columns = ['ORG_CODE', 'NAME', 'TIME']
        org_time_event = org_time_event[['ORG_CODE','TIME']].copy()

        org_result = pd.merge(org_event,org_tr_event,how = 'left',on = ['ORG_CODE'])
        org_result = pd.merge(org_result,org_time_event,how = 'left',on = ['ORG_CODE'])
        org_result['TRACK'] = org_result['TRACK'].fillna(0)
        org_result['TRACK'] = org_result['TRACK'].astype(float)
        org_result['TIME'] = org_result['TIME'].fillna(0)
        org_result['TIME'] = org_result['TIME'].astype(float)

        org_result['TRACK_RATE'] = org_result['TRACK']/org_result['EVENT']
        org_result['TIME_RATE'] = org_result['TIME']/org_result['EVENT']
        org_result = org_result[['ORG_CODE','TRACK_RATE','TIME_RATE']].copy()

        org_result['TIME_SCORE'] = 70 - (org_result['TIME_RATE']/0.01)
        org_result['TIME_SCORE'] = org_result.apply(lambda x:0 if (x['TIME_RATE'] > 2*time_err_rate or x['TIME_RATE'] > 0.7) else x['TIME_SCORE'],axis = 1)
        org_result['TIME_SCORE'] = org_result.apply(lambda x:70 if x['TIME_RATE'] < 0.01 else x['TIME_SCORE'],axis = 1)
        org_result['TIME_SCORE'] = org_result.apply(lambda x:70 if x['TIME_SCORE'] > 70 else x['TIME_SCORE'],axis = 1)

        org_result['TRACK_SCORE'] = 30 - ((org_result['TRACK_RATE']-0.1)/0.01*0.42)
        org_result['TRACK_SCORE'] = org_result.apply(lambda x:0 if (x['TRACK_RATE'] > 2*tr_err_rate or x['TRACK_RATE'] > 0.8) else x['TIME_SCORE'],axis = 1)
        org_result['TRACK_SCORE'] = org_result.apply(lambda x:30 if x['TRACK_RATE'] < 0.1 else x['TRACK_SCORE'],axis = 1)
        org_result['TRACK_SCORE'] = org_result.apply(lambda x:30 if x['TRACK_SCORE'] > 30 else x['TIME_SCORE'],axis = 1)

        org_result['SCORE'] = org_result['TIME_SCORE'] + org_result['TRACK_SCORE']
        org_result = org_result[['ORG_CODE','SCORE']].copy()

        sql=f"""\
        select DISTINCT ORG_CODE,TRADE_CODE,LABEL_CODE from BD_RISK_CORP_INFO_BASIC WHERE ISCURRENT = 1
        """
        corp_list = Read_Oracle().read_oracle(sql=sql, database=self.database)
        if len(corp_list) == 0:
            print('ERROR: 未获取到任何企业信息')
            return
        corp_list.columns = ['ORG_CODE','HG_ORG_ID','LABEL_CODE']
        corp_list['ORG_TYPE'] = corp_list['LABEL_CODE'].apply(lambda x: x.split(sep=',')[0])
        corp_list = corp_list[['ORG_CODE','HG_ORG_ID','ORG_TYPE']].copy()

        business_time = (datetime.date.today().replace(day=1) - datetime.timedelta(1)).replace(day=1)

        df_score = org_result[['ORG_CODE','SCORE']].copy()
        df_score = pd.merge(corp_list,df_score,how='left',on=['ORG_CODE'])
        df_score['SCORE'] = df_score['SCORE'].fillna(100)
        df_score['SCORE'] = df_score['SCORE'].astype(float)
        df_score['SCORE'] = df_score['SCORE'].apply(lambda x: round((x),2))
        df_score['MODEL_CODE'] = model_code
        df_score['BUSINESS_TIME'] = business_time
        df_score['BUSINESS_TIME'] = df_score['BUSINESS_TIME'].astype('datetime64')
        df_score = df_score.reset_index(drop=True)
        df_score = df_score.reset_index().rename(columns={'index': 'ID'})

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
            df_score['LABEL'] = '物流模型扣分'
            df_score['EVENT_NUMBER'] = 1
            df_score = df_score.reset_index(drop=True)
            df_score = df_score.reset_index().rename(columns={'index': 'ID'})
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_DTL', df_score, org_code = None,alarm = [model_code,''])

    def run_score_track(self):
        try:
            self.score_track()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    if params_global.is_test:
        child_task_id = sys.argv[1]
    else:
        child_task_id = sys.argv[1]
    org_code, params, base_time = read_log_table(child_task_id)
    score_tr = ScoreTr(org_code, params, base_time, child_task_id = child_task_id)
    score_tr.run_score_track()