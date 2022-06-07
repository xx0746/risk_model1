import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *

class DisplayDp3(object):
    def __init__(self, base_time, child_task_id):
        self.model_code = 'DISPLAY'
        self.child_model_code = 'DP3'
        self.child_task_id = child_task_id
        self.org_code = None
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        # startdt 是 base_time - 730 days，enddt为base_time
        self.startdt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-730)
        self.startdt = self.startdt.strftime('%Y%m%d')
        self.enddt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S").strftime('%Y%m%d') 
         

    def display_dp3(self):  
        sql=f'''SELECT 'I' AS IE_FLAG,T1.TIME as DATETIME,SUM(T1.RMB)/100000000 as TAX FROM 
        (SELECT TO_DATE(SUBSTR(A.APL_DATE_KEY,1,6),'YYYYMM') AS TIME ,SUM(B.REAL_TAX) AS RMB
        FROM {TableList.FT_I_DTL_SEA_PRE_RECORDED.value} A 
        JOIN {TableList.FT_I_DTL_TAX_INFO.value} B ON A.PRE_ENTRY_NO =B.ENTRY_NO
        WHERE A.APL_DATE_KEY >= {self.startdt} AND A.APL_DATE_KEY < {self.enddt} AND A.CUSTOMS_CODE ='2249'
        GROUP BY TO_DATE(SUBSTR(A.APL_DATE_KEY,1,6),'YYYYMM')
        UNION ALL
        SELECT TO_DATE(SUBSTR(A.APL_DATE_KEY,1,6),'YYYYMM') AS TIME ,SUM(B.REAL_TAX) AS RMB
        FROM {TableList.FT_I_DTL_OTR_PRE_RECORDED.value} A 
        JOIN {TableList.FT_I_DTL_TAX_INFO.value} B ON A.PRE_ENTRY_NO =B.ENTRY_NO
        WHERE A.APL_DATE_KEY >= {self.startdt} AND A.APL_DATE_KEY < {self.enddt} AND A.CUSTOMS_CODE ='2249'
        GROUP BY TO_DATE(SUBSTR(A.APL_DATE_KEY,1,6),'YYYYMM')) T1
        GROUP BY T1.TIME'''
        t5 = Read_Oracle().read_oracle(sql=sql,database='dbdw')

        sql=f'''SELECT 'E' AS IE_FLAG as DATETIME,T1.TIME,SUM(T1.RMB)/100000000 as TAX FROM 
        (SELECT SUBSTR(A.APL_DATE_KEY,1,6) AS TIME ,SUM(B.REAL_TAX)/100000000 AS RMB
        FROM {TableList.FT_E_DTL_SEA_PRE_RECORDED.value} A 
        JOIN {TableList.FT_E_DTL_TAX_INFO.value} B ON A.PRE_ENTRY_NO =B.ENTRY_NO
        WHERE A.APL_DATE_KEY >= {self.startdt} AND A.APL_DATE_KEY < {self.enddt} AND A.CUSTOMS_CODE ='2249'
        GROUP BY SUBSTR(A.APL_DATE_KEY,1,6)
        UNION ALL
        SELECT SUBSTR(A.APL_DATE_KEY,1,6) AS TIME ,SUM(B.REAL_TAX) AS RMB
        FROM {TableList.FT_E_DTL_OTR_PRE_RECORDED.value} A 
        JOIN {TableList.FT_E_DTL_TAX_INFO.value} B ON A.PRE_ENTRY_NO =B.ENTRY_NO
        WHERE A.APL_DATE_KEY >= {self.startdt} AND A.APL_DATE_KEY < {self.enddt} AND A.CUSTOMS_CODE ='2249'
        GROUP BY SUBSTR(A.APL_DATE_KEY,1,6)) T1
        GROUP BY T1.TIME
        '''
    #     t6 = Read_Oracle().read_oracle(sql=sql,database='dbdw')
        
    #     df_3=pd.concat([t5,t6])
        t5.reset_index(inplace=True,drop=True)

        df_final = t5.copy()

        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_final['CHECK_TIME']=now
        df_final['CHECK_TIME']= pd.to_datetime(df_final['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        df_final = df_final.reset_index().rename(columns = {'index':'ID'})
        # df_final['ORG_CODE'] = df_final['ORG_CODE'].astype(str)
        df_final['CHECK_TIME'] = df_final['CHECK_TIME'].astype('datetime64')
        df_final['DATETIME'] = df_final['DATETIME'].astype('datetime64')
        df_final['IE_FLAG'] = df_final['IE_FLAG'].astype(str)
        df_final['TAX'] = df_final['TAX'].astype(float)

        # 写入
        Write_Oracle().write_oracle(f'{TableList.MX_DISPLAY_TAX.value}',df_final,org_code=None,alarm=None)

    def run_display_dp3(self):
        try:
            self.display_dp3()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = 'c_dp3'
    org_code, param_json, basetime = read_log_table(child_task_id)
    DisplayDp3(base_time = basetime, child_task_id=child_task_id).run_display_dp3()