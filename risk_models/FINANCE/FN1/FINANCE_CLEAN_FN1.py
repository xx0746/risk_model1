import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *

class FinanceCleanFn1(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'FINANCE'
        self.child_model_code = 'FN1'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.startdt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-730)
        self.startdt = self.startdt.strftime('%Y-%m-%d')
        self.enddt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')  

    def clean_fn1(self):  
        # 读数
        sql=f"""\
        select * from {TableList.EMS_FINANCE_INFO.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' AND STARTDT >= DATE'{self.startdt}' AND STARTDT <= DATE'{self.enddt}' \
        """
        EMS_FINANCE_INFO=Read_Oracle().read_oracle(sql=sql,database='dbods')
        
        # 应加入公司ID筛选 记得加 CAPXACTION NOT LIKE 'D' AND
        sql=f"""\
        select DISTINCT BATCH_NO,BATCH_TYPE from {TableList.EMS_MANUFACTURE_TOTAL.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' AND STARTDT >= DATE'{self.startdt}' AND STARTDT <= DATE'{self.enddt}' \
        """   
        EMS_MANUFACTURE_TOTAL=Read_Oracle().read_oracle(sql=sql,database='dbods') 
        EMS_MANUFACTURE_TOTAL.columns = ['BATCH_NO','BATCH_TYPE']

        # 以下为数据类型转换的，正式部署应删除
        EMS_FINANCE_INFO['TRADE_TOTAL'] = EMS_FINANCE_INFO['TRADE_TOTAL'].astype(float)

        EMS_FINANCE_INFO.dropna(axis=0,subset = ["OBJ_CODE"],inplace=True)
        EMS_FINANCE_INFO['OBJ_CODE'] = EMS_FINANCE_INFO['OBJ_CODE'].apply( lambda  x : x[-3:])
        df = pd.merge(EMS_FINANCE_INFO,EMS_MANUFACTURE_TOTAL[['BATCH_NO','BATCH_TYPE']].drop_duplicates(),left_on='OBJ_CODE',
                right_on='BATCH_NO',how='left')
        df['BATCH_TYPE'] = df['BATCH_TYPE'].fillna('N/A')
        
        t1=df[['OBJ_CODE','ORG_CODE','TRADE_TOTAL','BATCH_TYPE']].copy()
        t2=t1.groupby(['OBJ_CODE','ORG_CODE','BATCH_TYPE'])['TRADE_TOTAL'].sum().reset_index()
        df_result = t2.copy()

        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result['CHECK_TIME']=now
        df_result['CHECK_TIME']= pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        df_result = df_result.reset_index().rename(columns = {'index':'ID'})
        df_result['ORG_CODE'] = df_result['ORG_CODE'].astype(str)
        df_result['CHECK_TIME'] = df_result['CHECK_TIME'].astype('datetime64')
        df_result['OBJ_CODE'] = df_result['OBJ_CODE'].astype(str)
        df_result['BATCH_TYPE'] = df_result['BATCH_TYPE'].astype(str)
        df_result['CUSTOMER_CODE'] = 'FTA_LG'

        # 写入
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_DETAIL_FINANCE_FN1.value}',df_result,org_code=self.org_code,alarm=None)
        
        # 图表显示
        t3=df[['OBJ_CODE','ORG_CODE','TRADE_TOTAL','BATCH_TYPE','KSTAR']].copy()
        if self.org_code == '91310000132612172J':
            k_list = ['外协费-加工','外协费-机体结构','材料费-半成品','材料费-原材料-化工','材料费-原材料-复合材料',
                  '材料费-原材料-成品件','材料费-原材料-机电配件','材料费-原材料-标准件','材料费-原材料-油料',
                  '材料费-原材料-结构件','材料费-原材料-金属','材料费-原材料-非金属','材料费-周转材料-劳防用品']
            t3['KSTAR'] = t3['KSTAR'].apply(lambda x : '-'.join(x.split('-')[1:]))
            t3 = t3[t3['KSTAR'].apply(lambda x : x in k_list)]
        result_display = t3.groupby(['OBJ_CODE','ORG_CODE','BATCH_TYPE','KSTAR'])['TRADE_TOTAL'].sum().reset_index()
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        result_display['CHECK_TIME']=now
        result_display['CHECK_TIME']= pd.to_datetime(result_display['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        result_display = result_display.reset_index().rename(columns = {'index':'ID'})
        result_display['ORG_CODE'] = result_display['ORG_CODE'].astype(str)
        result_display['CHECK_TIME'] = result_display['CHECK_TIME'].astype('datetime64')
        result_display['OBJ_CODE'] = result_display['OBJ_CODE'].astype(str)
        result_display['BATCH_TYPE'] = result_display['BATCH_TYPE'].astype(str)
        result_display['CUSTOMER_CODE'] = 'FTA_LG'
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_GRAPH_FINANCE_FN1.value}',result_display,org_code=self.org_code,alarm=None)

    def run_clean_fn1(self):
        try:
            self.clean_fn1()
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
    FinanceCleanFn1(org_code, params=param_json, base_time = basetime, child_task_id=child_task_id).run_clean_fn1()