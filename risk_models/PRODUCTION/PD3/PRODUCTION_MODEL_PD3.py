import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class ProductionModelPd3(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'Production'
        self.child_model_code = 'PD3'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        
        # 分数为每百条错误率的扣分，如：平均每100条中有一条错误则扣1分，默认为[0,-1]
        self.score_crit = json.loads(params)['score_crit']


    def model_pd3(self):
        sql=f"""\
        select * from {TableList.BD_RISK_DETAIL_PRODUCTION_PD3.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' and CUSTOMER_CODE = 'FTA_LG' \
        """
        detail_data = Read_Oracle().read_oracle(sql=sql,database='dbdm')

        detail_data = detail_data.drop(columns = ['ID','ISCURRENT','LASTUPDATE'])

        batch_cop_type = detail_data[['BATCH_TYPE','COP_G_NO']].copy().drop_duplicates()

        detail_data['THR_SUP'] = None
        detail_data['THR_INF'] = None
        batch_cop_type = detail_data[['BATCH_TYPE','COP_G_NO']].copy().drop_duplicates()

        for index,row in batch_cop_type.iterrows():

            temp_data = detail_data[(detail_data['BATCH_TYPE']==row['BATCH_TYPE'])&(detail_data['COP_G_NO']==row['COP_G_NO'])].copy()

            Q3 = temp_data['RATIO'].quantile(0.75)
            Q1 = temp_data['RATIO'].quantile(0.25)
            IQR = Q3 - Q1
            thr_sup = Q3 + 1.5*IQR
            thr_inf = Q1 - 1.5*IQR

            detail_data.loc[(detail_data['BATCH_TYPE']==row['BATCH_TYPE'])&(detail_data['COP_G_NO']==row['COP_G_NO']),'THR_SUP'] = thr_sup
            detail_data.loc[(detail_data['BATCH_TYPE']==row['BATCH_TYPE'])&(detail_data['COP_G_NO']==row['COP_G_NO']),'THR_INF'] = thr_inf

            temp_data = None

        detail_data['RISK_LABEL'] = '数据正常'
        detail_data['SCORE'] = self.score_crit[0]
        data_length = detail_data.shape[0]

        detail_data.loc[(detail_data['RATIO']>detail_data['THR_SUP'])|(detail_data['RATIO']<detail_data['THR_INF']),'RISK_LABEL'] = '领用数量异常'
        detail_data.loc[(detail_data['RATIO']>detail_data['THR_SUP'])|(detail_data['RATIO']<detail_data['THR_INF']),'SCORE'] = (self.score_crit[1]/data_length*100)

        df_result = detail_data[['ORG_CODE', 'WO_NO', 'RLT_BILL_DETAIL_SEQNO', 'COP_G_NO', 'BATCH_TYPE','OUTSTOCK_NUM', 'CONSUME_NUM', 'BACKSTOCK_NUM', 'RISK_LABEL', 'SCORE']].copy()
#         df_result = df_result[df_result['RISK_LABEL']!='数据正常']
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result['CHECK_TIME']=now
        df_result['CHECK_TIME']= pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')

        df_result = df_result.reset_index()

        df_result = df_result.drop(columns=['index'])

        # 格式确认，添加ID列
        df_result['ORG_CODE'] = df_result['ORG_CODE'].astype(str)
        df_result['WO_NO'] = df_result['WO_NO'].astype(str)
        df_result['RLT_BILL_DETAIL_SEQNO'] = df_result['RLT_BILL_DETAIL_SEQNO'].astype(str)
        df_result['COP_G_NO'] = df_result['COP_G_NO'].astype('str')
        df_result['BATCH_TYPE'] = df_result['BATCH_TYPE'].astype('str')
        df_result = df_result.reset_index().rename(columns = {'index':'ID'})
        df_result['OUTSTOCK_NUM'] = df_result['OUTSTOCK_NUM'].astype(float)
        df_result['CONSUME_NUM'] = df_result['CONSUME_NUM'].astype(float)
        df_result['BACKSTOCK_NUM'] = df_result['BACKSTOCK_NUM'].astype(float)
        df_result['CHECK_TIME'] = df_result['CHECK_TIME'].astype('datetime64')
        df_result['RISK_LABEL'] = df_result['RISK_LABEL'].astype('str')
        df_result['SCORE'] = df_result['SCORE'].astype('float')
        df_result['CUSTOMER_CODE'] = 'FTA_LG'

        # print(df_result['SCORE'].sum())

        Write_Oracle().write_oracle(f'{TableList.BD_RISK_RESULT_PRODUCTION_PD3.value}',df_result,org_code=self.org_code,alarm=None)
        
        # 整理预警明细数据，并写入数据库
        RISK_ALARM = df_result[df_result['SCORE'] != 0].groupby(['RISK_LABEL'], as_index=False)['ID'].count()
        RISK_ALARM = RISK_ALARM.rename(columns={'ID':'ALARM_NUMBER'})
        RISK_ALARM['ALARM_REASON'] = '发现' + RISK_ALARM['ALARM_NUMBER'].astype('str') + '起' + RISK_ALARM['RISK_LABEL'] + '事件'
        RISK_ALARM['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        RISK_ALARM['ORG_CODE'] = self.org_code
        RISK_ALARM['MODEL_CODE'] = 'PRODUCTION'
        RISK_ALARM['CHILD_MODEL_CODE'] = 'PD3'
        RISK_ALARM['ID'] = range(len(RISK_ALARM))
        RISK_ALARM = RISK_ALARM[['ID','ORG_CODE','MODEL_CODE','CHILD_MODEL_CODE','ALARM_REASON','ALARM_NUMBER','CHECK_TIME']].copy()
        RISK_ALARM['CUSTOMER_CODE'] = 'FTA_LG'
        
        if RISK_ALARM.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle(f'{TableList.BD_RISK_ALARM_ITEM.value}',RISK_ALARM, org_code = self.org_code, alarm = ['PRODUCTION','PD3'])
            

    def run_model_pd3(self):
        try:
            self.model_pd3()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '0001_00112'
    org_code, param_json, basetime = read_log_table(child_task_id)
    ProductionModelPd3(org_code, params=param_json, base_time=basetime, child_task_id = child_task_id).run_model_pd3()
