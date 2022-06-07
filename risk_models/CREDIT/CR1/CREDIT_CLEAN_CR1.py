import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class CreditCleanCr1(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'CREDIT'
        self.child_model_code = 'CR1'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.startdt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-730)
        self.startdt = self.startdt.strftime('%Y-%m-%d')
        self.enddt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')       


    def clean_cr1(self):
        p0=f'''select CORP_NAME,UNI_SC_ID from {TableList.ft_gov_dtl_corp_info.value} WHERE UNI_SC_ID LIKE '{self.org_code}'  '''
        corp_list = Read_Oracle().read_oracle(sql= p0, database = 'dbdw')
        org_name = corp_list.loc[0,'CORP_NAME']

        p1=f"""select SSQYMC,AH,SFZS,SFJA,AY,YG,BG,DSR,LARQ,JAFS from {TableList.MX_SHESU.value} ORDER BY LASTUPDATEDDT DESC"""
        shesu = Read_Oracle().read_oracle(sql= p1, database = 'dbods')
        shesu = shesu.drop_duplicates(subset=['AH'])

        p2=f"""select SSQYMC,AH,ZXYJWH,BZXRLXQK,FBSJ from {TableList.MX_SHIXIN.value} ORDER BY LASTUPDATEDDT DESC"""
        shixin = Read_Oracle().read_oracle(sql= p2, database = 'dbods')
        shixin = shixin.drop_duplicates(subset=['AH'])

        def role_test(row):
            yg_str = row['YG']
            bg_str = row['BG']
            dsr_str = row['DSR']
            corp_str = row['CORP_NAME']
            if not pd.isnull(bg_str):
                x = re.split(',',bg_str)
                if corp_str in x:
                    return '被告'
                elif not pd.isnull(yg_str):
                    x = re.split(',',yg_str)
                    if corp_str in x:
                        return '原告'
                    elif not pd.isnull(dsr_str):
                        x = re.split(',',dsr_str)
                        if corp_str in x:
                            return '第三人'
                        else:
                            return None

        t1 = pd.merge(corp_list,shesu,how='inner',left_on=['CORP_NAME'],right_on=['SSQYMC'])
        t1 = t1.drop(columns=['SSQYMC'])
        if t1.shape[0] != 0:
            t1['ROLE'] = t1.apply(role_test,axis=1)
            t1 = t1.drop(columns=['YG','BG','DSR'])
            t1.loc[t1['SFJA']=='否','JAFS'] = '未结案'
            t1.loc[t1['SFZS']=='是','EVENT_TYPE'] = '涉诉再审'
            t1.loc[t1['SFZS']=='否','EVENT_TYPE'] = '涉诉一审'
            t1 = t1.drop(columns=['SFZS','SFJA'])
            t1.columns = ['ORG_NAME','ORG_CODE','CASE_ID','CASE_INFO','CASE_TIME','STATUS','ROLE','EVENT_TYPE']

        t2 = pd.merge(corp_list,shixin,how='inner',left_on=['CORP_NAME'],right_on=['SSQYMC'])
        t2 = t2.drop(columns=['SSQYMC'])

        if t2.shape[0] != 0:
            # t2 = shixin.copy()
            # t2.rename(columns = {'SSQYMC':'CORP_NAME'},inplace=True)
            # t2['UNI_SC_ID'] = '111111111'
            # t2 = t2[['CORP_NAME', 'UNI_SC_ID', 'AH', 'ZXYJWH', 'BZXRLXQK', 'FBSJ']].copy()

            t2.columns = ['ORG_NAME','ORG_CODE','CASE_ID','CASE_INFO','STATUS','CASE_TIME']
            t2['EVENT_TYPE'] = '失信被执行'
            t2['ROLE'] = '被执行人'

        if t1.shape[0] != 0:
            if t2.shape[0] != 0:
                df_result = pd.concat([t1,t2])
            else:
                df_result = t1.copy()
        elif t2.shape[0] != 0:
            df_result = t2.copy()
        else:
            df_result = pd.DataFrame(columns = ['ORG_NAME','ORG_CODE','CASE_ID','CASE_INFO','CASE_TIME','STATUS','ROLE','EVENT_TYPE'])
            df_result = df_result.append({'ORG_NAME':org_name,'ORG_CODE':self.org_code,'CASE_ID':'' ,'CASE_INFO':'' ,'CASE_TIME':None ,'STATUS':'' ,'ROLE':'' ,'EVENT_TYPE':'' },ignore_index = True)

        df_result = df_result.reset_index().rename(columns = {'index':'ID'})
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result['CHECK_TIME']=now
        df_result['CHECK_TIME']= pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        df_result['CASE_TIME'] = df_result['CASE_TIME'].astype('datetime64')
        df_result['CUSTOMER_CODE'] = 'FTA_LG'

        Write_Oracle().write_oracle(f'{TableList.BD_RISK_DETAIL_CREDIT_CR1.value}',df_result,org_code=self.org_code,alarm=None)


    def run_clean_cr1(self):
        try:
            self.clean_cr1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = 'c_cr1_1'
    org_code, param_json, base_time = read_log_table(child_task_id)
    CreditCleanCr1(org_code, params=param_json, base_time = base_time, child_task_id=child_task_id).run_clean_cr1()