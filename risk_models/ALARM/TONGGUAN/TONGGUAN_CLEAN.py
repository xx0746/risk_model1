import sys, os
from os import path

import pandas as pd

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\WorkSpace\Pycharm_Space\bdrisk_model\risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class TongGuanClean(object):

    def __init__(self,org_code,child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status = None)
        ##param
        self.org_code = org_code
        ####model run time
        self.now = datetime.datetime.now()
        ####data query end time
        self.query_time = datetime.datetime(self.now.year, self.now.month, 1)


    def clean_TongGuan(self):

        sql_cop = """select distinct ORG_CODE,ORG_NAME,TRADE_CODE,CUSTOM_ID FROM {} WHERE ISCURRENT = 1 """.format('DW_CUS_RC.BD_RISK_CORP_INFO_BASIC')
        CORP_INFO = Read_Oracle().read_oracle(sql=sql_cop, database='dbdw')
        CORP_INFO.rename(columns={"ORG_CODE": "CORP_CREDIT_CODE", 'ORG_NAME': 'TRADE_NAME'}, inplace=True)
        TRADE_NM = dict(zip(CORP_INFO['CORP_CREDIT_CODE'], CORP_INFO['TRADE_NAME']))
        TRADE_cd = dict(zip(CORP_INFO['CORP_CREDIT_CODE'], CORP_INFO['TRADE_CODE']))
        CUSTOM_cd = dict(zip(CORP_INFO['CORP_CREDIT_CODE'], CORP_INFO['CUSTOM_ID']))

        sql = """ SELECT DISTINCT a.CUSTOM_ID,a.CREDIT_CODE,a.TRADE_NAME,a.TRADE_CODE,b.ENTRY_ID,b.I_E_DATE,I_E_FLAG FROM  {} a JOIN {} b ON a.CREDIT_CODE = b.CONSIGN_SCC
                    WHERE a.TRADE_NAME IS NOT NULL AND I_E_DATE IS NOT NULL AND ENTRY_ID IS NOT NULL AND b.DECL_PORT = '2249' and a.SOURCE = '2' AND STATUS = '5'
                    AND I_E_DATE > ADD_MONTHS(TO_DATE('{}','yyyy-mm-dd hh24:mi:ss'),-6)
                    AND I_E_DATE < TO_DATE('{}','yyyy-mm-dd hh24:mi:ss')""".format(
                    'N_EPZ_CUS.TRADE', 'LGSA.ENTRY_HEAD', self.query_time,self.query_time)

        Tong_Guan_Data = Read_Oracle().read_oracle(sql=sql, database='dbods')


        if Tong_Guan_Data.empty:
            Merge_Data = CORP_INFO.copy(deep = True)
            Merge_Data['ENTRY_COUNT'] = 0
            Merge_Data['MANUAL_ENTRY_COUNT'] = 0
            Merge_Data['C_SCORE'] = 50
        else:
            ##get the  ENTRY_ID count
            Count_Data = Tong_Guan_Data.groupby(['CUSTOM_ID','CREDIT_CODE','TRADE_NAME','TRADE_CODE'],as_index = False)['ENTRY_ID'].count()
            Count_Data = Count_Data.rename(columns={'ENTRY_ID':'ENTRY_COUNT'})
            ###get distinct CREDIT_CODE ENTRY_ID
            Distinct_ENTRY_DATA = Tong_Guan_Data[['CREDIT_CODE','ENTRY_ID']].drop_duplicates(subset = ['CREDIT_CODE','ENTRY_ID'], keep = 'first')
            sql2 = """SELECT DISTINCT ENTRY_ID FROM {} WHERE STEP_ID = '20000000' 
                        AND  PROC_RESULT IS NOT NULL AND CREATE_DATE > ADD_MONTHS(TO_DATE('{}','yyyy-mm-dd hh24:mi:ss'),-6)  
                        AND CREATE_DATE < TO_DATE('{}','yyyy-mm-dd hh24:mi:ss')""".format('LGSA.ENTRY_WORKFLOW',self.query_time,self.query_time)
            Manual_Review_Data = Read_Oracle().read_oracle(sql=sql2, database='dbods')

            if Manual_Review_Data.empty:
                Count_Data['MANUAL_ENTRY_COUNT'] = 0
                Merge_Data = Count_Data.copy(deep = True)
                Merge_Data['C_SCORE'] = 50
                Merge_Data.rename(columns={"CREDIT_CODE": "CORP_CREDIT_CODE"}, inplace=True)
            else:  ###不为空
                ###Merge Get CREDIT_CODE
                Manual_Review_Data = pd.merge(Manual_Review_Data, Distinct_ENTRY_DATA,on='ENTRY_ID',how='inner', suffixes=('_x', '_y'))
                Manual_Review_Count = Manual_Review_Data.groupby(['CREDIT_CODE'], as_index=False)['ENTRY_ID'].count()

                Manual_Review_Count = Manual_Review_Count.rename(columns={"CREDIT_CODE": "CORP_CREDIT_CODE",'ENTRY_ID': 'MANUAL_ENTRY_COUNT'})
                Count_Data.rename(columns={"CREDIT_CODE": "CORP_CREDIT_CODE"}, inplace=True)

                CORP_INFO_cp = CORP_INFO.copy(deep=True)
                CORP_INFO_cp = pd.DataFrame(CORP_INFO_cp['CORP_CREDIT_CODE'])
                Count_Data = pd.merge(CORP_INFO_cp, Count_Data, on='CORP_CREDIT_CODE', how='left',
                                      suffixes=('_x', '_y'))
                Count_Data['CUSTOM_ID'] = Count_Data['CORP_CREDIT_CODE'].map(CUSTOM_cd)
                Count_Data['TRADE_NAME'] = Count_Data['CORP_CREDIT_CODE'].map(TRADE_NM)
                Count_Data['TRADE_CODE'] = Count_Data['CORP_CREDIT_CODE'].map(TRADE_cd)
                Count_Data['ENTRY_COUNT'].fillna(0, inplace=True)
                ###merge
                Merge_Data = pd.merge(Count_Data, Manual_Review_Count,on='CORP_CREDIT_CODE',how='left', suffixes=('_x', '_y'))
                Merge_Data = Merge_Data.fillna(0)
                Merge_Data['C_SCORE'] = Merge_Data[['ENTRY_COUNT', 'MANUAL_ENTRY_COUNT']].apply(lambda x: (50 * (1 - (x['MANUAL_ENTRY_COUNT']/x['ENTRY_COUNT'])) if (x['ENTRY_COUNT']) > 0 else 50 ),axis = 1)
                Merge_Data['C_SCORE'].fillna(50, inplace=True)

        ###查获率得分
        sql_se  = """SELECT DISTINCT CORP_CREDIT_CODE,TYPE_FIRST FROM {} WHERE CORP_CREDIT_CODE IS NOT NULL  AND STATUS = '18'
                            AND  STATISTICS_END_DATE > ADD_MONTHS(TO_DATE('{}','yyyy-mm-dd hh24:mi:ss'),-1)""".format('LGSA.PASS_MODEL_SEIZED_RATE',self.query_time, self.query_time)
        Seizure_Rate_Data = Read_Oracle().read_oracle(sql=sql_se, database='dbods')



        if Seizure_Rate_Data.empty:
            if Tong_Guan_Data.empty:
                Merge_Data['TYPE_FIRST'] = 50 ###默认50分
                #Result_Data = CORP_INFO.copy(deep=True)
                need_list = ['CUSTOM_ID', 'CORP_CREDIT_CODE', 'TRADE_NAME', 'TRADE_CODE', 'ENTRY_COUNT',
                             'MANUAL_ENTRY_COUNT', 'C_SCORE', 'TYPE_FIRST']
                Result_Data = Merge_Data[need_list]
            else:
                Merge_Data['TYPE_FIRST'] = 50
                Result_Data = Merge_Data.copy(deep = True)
        else:
            Seizure_Rate_Data['TYPE_FIRST'] = Seizure_Rate_Data['TYPE_FIRST'].map(float)
            Result_Data = pd.merge(Merge_Data, Seizure_Rate_Data, on='CORP_CREDIT_CODE', how='left', suffixes=('_x', '_y'))
            Result_Data = Result_Data.fillna(50)

        if Result_Data.empty:
            print(self.org_code+':no data records')
        else: ###不为空 读取最后存储数据格式
            need_list = ['CUSTOM_ID','CORP_CREDIT_CODE','TRADE_NAME','TRADE_CODE','ENTRY_COUNT', 'MANUAL_ENTRY_COUNT','C_SCORE','TYPE_FIRST']
            Result_Data = Result_Data[need_list]
            CORP_INFO_code = pd.DataFrame(CORP_INFO['CORP_CREDIT_CODE'])
            Result_Data = pd.merge(CORP_INFO_code, Result_Data, on='CORP_CREDIT_CODE', how='left', suffixes=('_x', '_y'))
            Result_Data['TRADE_NAME'] = Result_Data['CORP_CREDIT_CODE'].map(TRADE_NM)
            Result_Data['TRADE_CODE'] = Result_Data['CORP_CREDIT_CODE'].map(TRADE_cd)
            Result_Data['ENTRY_COUNT'].fillna(0, inplace=True)
            Result_Data['MANUAL_ENTRY_COUNT'].fillna(0, inplace=True)
            Result_Data['C_SCORE'].fillna(50, inplace=True)
            Result_Data['TYPE_FIRST'].fillna(50, inplace=True)
            Result_Data['CUSTOM_ID'].fillna('2249', inplace=True)
            Result_Data = Result_Data.reset_index().rename(columns={'index': 'ID'})
            Result_Data['BUSINESS_END_TIME'] = self.query_time
            Result_Data['MODEL_TIME'] = self.now
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_DETAIL_PASS_CLEAN', Result_Data,None,alarm=None)

    def run_TongGuan(self):
        try:
            self.clean_TongGuan()
            exec_status = 1
        except Exception as e:
            errorStr = f'''{e}'''
            errorStr = errorStr.replace("'", " ")
            errorStr = errorStr[-3900:]
            logger.exception(errorStr)
            exec_status = 0
        finally:
            print(1)
            #Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()

if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    if params_global.is_test:
        child_task_id = '1ed78494e4b54b5f8d13521a0d417636'
    else:
        child_task_id = sys.argv[1]

    #org_code,param_json,BASE_TIME = read_log_table(child_task_id)
    TongGuanClean(None,child_task_id).run_TongGuan()











