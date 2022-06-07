import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *

class DisplayDp2(object):
    def __init__(self, base_time, child_task_id):
        self.model_code = 'DISPLAY'
        self.child_model_code = 'DP2'
        self.child_task_id = child_task_id
        self.org_code = None
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        # startdt 是 base_time - 730 days，enddt为base_time
        self.startdt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-730)
        self.startdt = self.startdt.strftime('%Y-%m-%d')
        self.enddt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d') 
         

    def display_dp2(self):  
        def date_standard(df):
            """
            des: 填充缺失的时间
            input df['col']
            output df_series
            """
            b=[]
            # date_begin=datetime.datetime(2020,10,1)
            # date_end=datetime.datetime(2021,11,1)
            date_begin=df.min()
            date_end=df.max()
            a=pd.date_range(date_begin,date_end,freq='MS',normalize=True)
            for i in a:  
                i=str(i)
            #     print(datetime.datetime.strptime(str(i),'%Y-%m-%d %H:%M:%S'))
            #     i=i.replace('31','01')
            #     i=i.replace('30','01')
            #     i=i.replace('29','01')
            #     i=i.replace('28','01')
                i=datetime.datetime.strptime(i,'%Y-%m-%d %H:%M:%S')
                b.append(i)
            date_serie=pd.DataFrame(b)
            date_serie=date_serie.rename(columns={0:'STANDARD_DATE'})
            return date_serie

        #     径予进口
        sql=f'''SELECT 'I' AS IE_FLAG,to_date(TO_CHAR(a.DCL_TIME,'YYYYMM'),'yyyymm') AS DATETIME, \
        COUNT(DISTINCT A.ORDER_NO) AS ENTRY_NUM, COUNT(DISTINCT A.BIZOP_ETPS_NO) AS TRADE_NUM, \
        SUM(B.DCL_TOTAL_AMT) AS MONEY,'径予' as type \
        FROM {TableList.BILL_DIR_BSC.value} A JOIN {TableList.BILL_DIR_DT.value} B \
        ON A.ID =B.PID WHERE A.DCL_TIME >= DATE '{self.startdt}' AND A.DCL_TIME< DATE'{self.enddt}' \
        GROUP BY to_date(TO_CHAR(a.DCL_TIME,'YYYYMM'),'yyyymm') \
        '''
        t3=Read_Oracle().read_oracle(sql=sql,database='dbods')

        # 径予出口
        sql=f'''\
        SELECT 'E' AS IE_FLAG,TO_DATE(TO_CHAR(A.DCL_TIME,'YYYYMM'),'yyyymm') AS DATETIME, \
        COUNT(DISTINCT A.ORDER_NO) AS ENTRY_NUM, COUNT(DISTINCT A.BIZOP_ETPS_NO) AS TRADE_NUM, \
        SUM(B.DCL_TOTAL_AMT) AS MONEY,'径予' as type \
        FROM {TableList.BILL_DIR_EXP_BSC.value} A JOIN {TableList.BILL_DIR_EXP_DT.value} B ON A.ID =B.PID \
        WHERE A.DCL_TIME >= DATE'{self.startdt}' AND A.DCL_TIME< DATE'{self.enddt}' \
        GROUP BY TO_DATE(TO_CHAR(A.DCL_TIME,'YYYYMM'),'yyyymm')'''
        t4=Read_Oracle().read_oracle(sql=sql,database='dbods')

        df_jingyu=pd.concat([t3,t4])
        df_jingyu.reset_index(inplace=True,drop=True)
        df_jingyu=df_jingyu.fillna(0)

        date_serie=date_standard(df_jingyu['DATETIME'])

        df_final_I=date_serie.merge(df_jingyu[df_jingyu['IE_FLAG']=='I'],left_on='STANDARD_DATE',right_on='DATETIME',how='left')
        df_final_I['IE_FLAG']='I';df_final_I['TYPE']='径予';df_final_I=df_final_I.fillna(0)
        df_final_E=date_serie.merge(df_jingyu[df_jingyu['IE_FLAG']=='E'],left_on='STANDARD_DATE',right_on='DATETIME',how='left')
        df_final_E['IE_FLAG']='E';df_final_E['TYPE']='径予';df_final_E=df_final_E.fillna(0)
        df_final=pd.concat([df_final_I,df_final_E],axis=0,ignore_index=True)
        df_final=df_final.drop(['DATETIME'],axis=1)
        df_final=df_final.rename(columns={'STANDARD_DATE':'DATETIME'})

        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_final['CHECK_TIME']=now
        df_final['CHECK_TIME']= pd.to_datetime(df_final['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        df_final = df_final.reset_index().rename(columns = {'index':'ID'})
        # df_final['ORG_CODE'] = df_final['ORG_CODE'].astype(str)
        df_final['CHECK_TIME'] = df_final['CHECK_TIME'].astype('datetime64')
        df_final['DATETIME'] = df_final['DATETIME'].astype('datetime64')
        df_final['IE_FLAG'] = df_final['IE_FLAG'].astype(str)
        df_final['ENTRY_NUM'] = df_final['ENTRY_NUM'].astype(float)
        df_final['TRADE_NUM'] = df_final['TRADE_NUM'].astype(float)
        df_final['MONEY'] = df_final['MONEY'].astype(float)
        df_final['TYPE'] = df_final['TYPE'].astype(str)

        # 写入
        Write_Oracle().write_oracle(f'{TableList.MX_DISPLAY_JINGYU.value}',df_final,org_code=None,alarm=None)

    def run_display_dp2(self):
        try:
            self.display_dp2()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = 'c_dp2'
    org_code, param_json, basetime = read_log_table(child_task_id)
    DisplayDp2(base_time = basetime, child_task_id=child_task_id).run_display_dp2()