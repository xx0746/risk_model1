import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class CreditModelCr1(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'CREDIT'
        self.child_model_code = 'CR1'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        # [无任何案件，涉诉原告，涉诉第三人，涉诉被告，涉诉未结案，被执行人]
        self.score_crit = json.loads(params)['score_crit']


    def model_cr1(self):
        p1=f"""select * from {TableList.BD_RISK_DETAIL_CREDIT_CR1.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' and CUSTOMER_CODE = 'FTA_LG' """
        CR_DETAIL = Read_Oracle().read_oracle(sql= p1, database = 'dbdm')
        

        CR_DETAIL = CR_DETAIL.drop(columns=['ID','CHECK_TIME','ISCURRENT','LASTUPDATE'])

        t1 = CR_DETAIL[CR_DETAIL['STATUS'] != '未结案'].copy()
        t2 = CR_DETAIL[CR_DETAIL['STATUS'] == '未结案'].copy()

        t2['RISK_LABEL'] = '涉诉未结案'
        t2['SCORE'] = self.score_crit[4]

        t1.loc[(t1['EVENT_TYPE']=='涉诉一审')&(t1['ROLE'] == '被告'),'RISK_LABEL'] = '涉诉一审，为被告'
        t1.loc[(t1['EVENT_TYPE']=='涉诉一审')&(t1['ROLE'] == '被告'),'SCORE'] = self.score_crit[3]
        t1.loc[(t1['EVENT_TYPE']=='涉诉再审')&(t1['ROLE'] == '被告'),'RISK_LABEL'] = '涉诉二审，为被告'
        t1.loc[(t1['EVENT_TYPE']=='涉诉再审')&(t1['ROLE'] == '被告'),'SCORE'] = self.score_crit[3]
        t1.loc[t1['ROLE'] == '原告','RISK_LABEL'] = '涉诉，为原告'
        t1.loc[t1['ROLE'] == '原告','SCORE'] = self.score_crit[1]
        t1.loc[t1['ROLE'] == '第三人','RISK_LABEL'] = '涉诉，为第三人'
        t1.loc[t1['ROLE'] == '第三人','SCORE'] = self.score_crit[2]
        t1.loc[t1['EVENT_TYPE'] == '失信被执行','RISK_LABEL'] = '为失信被执行人'
        t1.loc[t1['EVENT_TYPE'] == '失信被执行','SCORE'] = self.score_crit[5]

        df_result = pd.concat([t1,t2])
        df_result = df_result.reset_index(drop = True)
        if ((df_result.shape[0] == 1) & ((pd.isnull(df_result.loc[0,'CASE_ID'])|(df_result.loc[0,'CASE_ID'] == 'nan')))):
            df_result.loc[0,'RISK_LABEL'] = '无任何涉诉、失信记录'
            df_result.loc[0,'SCORE'] = self.score_crit[0]
            df_result.loc[0,'CASE_ID'] = ''
            df_result.loc[0,'CASE_INFO'] = ''
            df_result.loc[0,'STATUS'] = ''
            df_result.loc[0,'ROLE'] = ''
            df_result.loc[0,'EVENT_TYPE'] = ''

        df_result = df_result.reset_index().rename(columns = {'index':'ID'})
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result['CHECK_TIME']=now
        df_result['CHECK_TIME']= pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        df_result['CASE_TIME'] = df_result['CASE_TIME'].astype('datetime64')
        df_result['CUSTOMER_CODE'] = 'FTA_LG'

        Write_Oracle().write_oracle(f'{TableList.BD_RISK_RESULT_CREDIT_CR1.value}',df_result,org_code=self.org_code,alarm=None)
        
        # 整理预警明细数据，并写入数据库
        RISK_ALARM = df_result[df_result['RISK_LABEL'] != '无任何涉诉、失信记录'].groupby(['RISK_LABEL'], as_index=False)['ID'].count()
        RISK_ALARM = RISK_ALARM.rename(columns={'ID':'ALARM_NUMBER'})
        RISK_ALARM['ALARM_REASON'] = '发现' + RISK_ALARM['ALARM_NUMBER'].astype('str') + '起' + RISK_ALARM['RISK_LABEL'] + '事件'
        RISK_ALARM['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        RISK_ALARM['ORG_CODE'] = self.org_code
        RISK_ALARM['MODEL_CODE'] = 'CREDIT'
        RISK_ALARM['CHILD_MODEL_CODE'] = 'CR1'
        RISK_ALARM['ID'] = range(len(RISK_ALARM))
        RISK_ALARM = RISK_ALARM[['ID','ORG_CODE','MODEL_CODE','CHILD_MODEL_CODE','ALARM_REASON','ALARM_NUMBER','CHECK_TIME']].copy()
        RISK_ALARM['CUSTOMER_CODE'] = 'FTA_LG'

        if RISK_ALARM.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle(f'{TableList.BD_RISK_ALARM_ITEM.value}',RISK_ALARM, org_code = self.org_code, alarm = ['CREDIT','CR1'])


    def run_model_cr1(self):
        try:
            self.model_cr1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = 'c_cr1_2'
    org_code, param_json, base_time = read_log_table(child_task_id)
    CreditModelCr1(org_code, params=param_json, base_time = base_time, child_task_id=child_task_id).run_model_cr1()