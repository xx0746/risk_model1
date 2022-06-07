import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class FinanceModelFn1(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'FINANCE'
        self.child_model_code = 'FN1'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code

        # 默认为 [1,-1]
        self.score_crit = json.loads(params)['score_crit']

    def model_fn1(self):

        sql=f"""\
        select * from {TableList.BD_RISK_DETAIL_FINANCE_FN1.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' and CUSTOMER_CODE = 'FTA_LG' \
        """
        model_clean_data=Read_Oracle().read_oracle(sql=sql,database='dbdm')
        model_data = model_clean_data.drop(columns=['ID','ISCURRENT','CHECK_TIME','LASTUPDATE'])

        model_data['THR_SUP'] = None
        model_data['THR_INF'] = None
        batch_type = model_data['BATCH_TYPE'].copy().drop_duplicates()

        for batch_type_x in batch_type:
            
            temp_data = model_data[model_data['BATCH_TYPE']==batch_type_x].copy()
            
            Q3 = temp_data['TRADE_TOTAL'].quantile(0.75)
            Q1 = temp_data['TRADE_TOTAL'].quantile(0.25)
            IQR = Q3 - Q1
            thr_sup = Q3 + 1.5*IQR
            thr_inf = Q1 - 1.5*IQR
            
            model_data.loc[model_data['BATCH_TYPE']==batch_type_x,'THR_SUP'] = thr_sup
            model_data.loc[model_data['BATCH_TYPE']==batch_type_x,'THR_INF'] = thr_inf
            
            temp_data = None
        
        model_data['RISK_LABEL'] = '数据正常'
        model_data['SCORE'] = self.score_crit[0]
        data_length = model_data.shape[0]

        model_data.loc[(model_data['TRADE_TOTAL']>model_data['THR_SUP'])|(model_data['TRADE_TOTAL']<model_data['THR_INF']),'RISK_LABEL'] = '成品成本异常'
        model_data.loc[(model_data['TRADE_TOTAL']>model_data['THR_SUP'])|(model_data['TRADE_TOTAL']<model_data['THR_INF']),'SCORE'] = (self.score_crit[1]/data_length*100)

        df_result = model_data[['ORG_CODE','OBJ_CODE','BATCH_TYPE','TRADE_TOTAL','RISK_LABEL','SCORE']].copy()
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result['CHECK_TIME']=now
        df_result['CHECK_TIME']= pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        df_result = df_result.reset_index().rename(columns = {'index':'ID'})
        df_result['ORG_CODE'] = df_result['ORG_CODE'].astype(str)
        df_result['CHECK_TIME'] = df_result['CHECK_TIME'].astype('datetime64')
        df_result['OBJ_CODE'] = df_result['OBJ_CODE'].astype(str)
        df_result['BATCH_TYPE'] = df_result['BATCH_TYPE'].astype(str)
        df_result['CUSTOMER_CODE'] = 'FTA_LG'

        # print(df_result['SCORE'].sum())

        # 写入
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_RESULT_FINANCE_FN1.value}',df_result,org_code=self.org_code,alarm=None)
        
        # 整理预警明细数据，并写入数据库
        RISK_ALARM = df_result[df_result['SCORE'] != 0].groupby(['RISK_LABEL'], as_index=False)['ID'].count()
        RISK_ALARM = RISK_ALARM.rename(columns={'ID':'ALARM_NUMBER'})
        RISK_ALARM['ALARM_REASON'] = '发现' + RISK_ALARM['ALARM_NUMBER'].astype('str') + '起' + RISK_ALARM['RISK_LABEL'] + '事件'
        RISK_ALARM['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        RISK_ALARM['ORG_CODE'] = self.org_code
        RISK_ALARM['MODEL_CODE'] = 'FINANCE'
        RISK_ALARM['CHILD_MODEL_CODE'] = 'FN1'
        RISK_ALARM['ID'] = range(len(RISK_ALARM))
        RISK_ALARM = RISK_ALARM[['ID','ORG_CODE','MODEL_CODE','CHILD_MODEL_CODE','ALARM_REASON','ALARM_NUMBER','CHECK_TIME']].copy()
        RISK_ALARM['CUSTOMER_CODE'] = 'FTA_LG'
        
        if RISK_ALARM.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle(f'{TableList.BD_RISK_ALARM_ITEM.value}',RISK_ALARM, org_code = self.org_code, alarm = ['FINANCE','FN1'])
            

    def run_model_fn1(self):
        try:
            self.model_fn1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '0001_00062'
    org_code, param_json, basetime = read_log_table(child_task_id)
    FinanceModelFn1(org_code, params=param_json, base_time = basetime, child_task_id=child_task_id).run_model_fn1()
