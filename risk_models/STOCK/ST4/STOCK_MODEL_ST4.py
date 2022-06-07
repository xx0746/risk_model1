import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class StockModelSt4(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'STOCK'
        self.child_model_code = 'ST4'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        # 默认为None
        js_para = json.loads(params)
        if 'rate_thr_sup' in js_para:
            self.rate_thr_sup = js_para['rate_thr_sup']
        else:
            self.rate_thr_sup = None
        if 'rate_thr_inf' in js_para:
            self.rate_thr_inf = js_para['rate_thr_inf']
        else:
            self.rate_thr_inf = None
        # 默认为 [0,-0.75,-0.75]
        if 'score_crit' in js_para:
            self.score_crit = js_para['score_crit']

    def model_st4(self):

        # 应加入公司ID筛选，默认取ISCURRENT为1的数据，
        sql=f"""\
        select * from {TableList.BD_RISK_DETAIL_STOCK_ST4.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' and CUSTOMER_CODE = 'FTA_LG' \
        """     
        MODEL_DETAIL = Read_Oracle().read_oracle(sql=sql,database='dbdm')
        MODEL_DETAIL = MODEL_DETAIL.drop(columns = ['ID','ISCURRENT','LASTUPDATE'])

        # 若阈值为空，则设为RATE的90分位数与10分位数
        if pd.isnull(self.rate_thr_sup):
            rate_thr_sup = MODEL_DETAIL['RATE'].quantile(0.9)
        else:
            rate_thr_sup = self.rate_thr_sup

        if pd.isnull(self.rate_thr_inf):
            rate_thr_inf = MODEL_DETAIL['RATE'].quantile(0.1)
        else:
            rate_thr_inf = self.rate_thr_inf

        MODEL_DETAIL['RISK_LABEL'] = '数据正常'
        MODEL_DETAIL['SCORE'] = self.score_crit[0]

        data_length = MODEL_DETAIL.shape[0]

        # 异常数据每百条扣0.75分
        MODEL_DETAIL.loc[MODEL_DETAIL['RATE']<rate_thr_inf,'RISK_LABEL'] = '入库价值过低'
        MODEL_DETAIL.loc[MODEL_DETAIL['RATE']<rate_thr_inf,'SCORE'] = (self.score_crit[1]/data_length*100)

        MODEL_DETAIL.loc[MODEL_DETAIL['RATE']>rate_thr_sup,'RISK_LABEL'] = '入库价值过高'
        MODEL_DETAIL.loc[MODEL_DETAIL['RATE']>rate_thr_sup,'SCORE'] = (self.score_crit[1]/data_length*100)

#         # 剔除非负分的数据（即只保留异常数据）
#         df_result = MODEL_DETAIL[MODEL_DETAIL['SCORE']<0].copy()
        df_result = MODEL_DETAIL.copy()

        # 只保留需要的字段
        df_result = df_result[['ORG_CODE','ORDER_NO','COP_G_NO','SIGN_DT','QTY_ORDER', 'QTY_DELIV', 'QTY_STOCK', 'PRICE_ORDER','PRICE_DELIV',\
                            'PRICE_STOCK','RISK_LABEL','SCORE']].copy()

        # 添加CHECK_TIME
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result['CHECK_TIME']=now
        df_result['CHECK_TIME']= pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')

        # 格式确认，重设INDEX后添加ID列
        df_result = df_result.reset_index().rename(columns = {'index':'ID'})
        df_result['ORG_CODE'] = df_result['ORG_CODE'].astype(str)
        df_result['ORDER_NO'] = df_result['ORDER_NO'].astype(str)
        df_result['COP_G_NO'] = df_result['COP_G_NO'].astype(str)
        df_result['CHECK_TIME'] = df_result['CHECK_TIME'].astype('datetime64')
        df_result['SIGN_DT'] = df_result['SIGN_DT'].astype('datetime64')
        df_result['QTY_STOCK'] = df_result['QTY_STOCK'].astype(float)
        df_result['QTY_DELIV'] = df_result['QTY_DELIV'].astype(float)
        df_result['QTY_ORDER'] = df_result['QTY_ORDER'].astype(float)
        df_result['PRICE_STOCK'] = df_result['PRICE_STOCK'].astype(float)
        df_result['PRICE_DELIV'] = df_result['PRICE_DELIV'].astype(float)
        df_result['PRICE_ORDER'] = df_result['PRICE_ORDER'].astype(float)
        df_result['SCORE'] = df_result['SCORE'].astype(float)
        df_result['RISK_LABEL'] = df_result['RISK_LABEL'].astype(str)
        df_result['CUSTOMER_CODE'] = 'FTA_LG'

        # 写入
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_RESULT_STOCK_ST4.value}',df_result,org_code=self.org_code,alarm=None)
        
        # 整理预警明细数据，并写入数据库
        RISK_ALARM = df_result[df_result['SCORE'] != 0].groupby(['RISK_LABEL'], as_index=False)['ID'].count()
        RISK_ALARM = RISK_ALARM.rename(columns={'ID':'ALARM_NUMBER'})
        RISK_ALARM['ALARM_REASON'] = '发现' + RISK_ALARM['ALARM_NUMBER'].astype('str') + '起' + RISK_ALARM['RISK_LABEL'] + '事件'
        RISK_ALARM['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        RISK_ALARM['ORG_CODE'] = self.org_code
        RISK_ALARM['MODEL_CODE'] = 'STOCK'
        RISK_ALARM['CHILD_MODEL_CODE'] = 'ST4'
        RISK_ALARM['ID'] = range(len(RISK_ALARM))
        RISK_ALARM = RISK_ALARM[['ID','ORG_CODE','MODEL_CODE','CHILD_MODEL_CODE','ALARM_REASON','ALARM_NUMBER','CHECK_TIME']].copy()
        RISK_ALARM['CUSTOMER_CODE'] = 'FTA_LG'
        
        if RISK_ALARM.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle(f'{TableList.BD_RISK_ALARM_ITEM.value}',RISK_ALARM, org_code = self.org_code, alarm = ['STOCK','ST4'])

    def run_model_st4(self):
        try:
            self.model_st4()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '42990dc05aae4b7ebebda3abbca54cbb'
    org_code, param_json, basetime = read_log_table(child_task_id)
    StockModelSt4(org_code, params=param_json, base_time=basetime, child_task_id=child_task_id).run_model_st4()