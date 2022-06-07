import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class ProductionCleanPd3(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'Production'
        self.child_model_code = 'PD3'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.startdt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-90)
        self.startdt = self.startdt.strftime('%Y-%m-%d')
        self.startdt2 = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-121)
        self.startdt2 = self.startdt2.strftime('%Y-%m-%d')
        self.enddt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')
        


    def clean_pd3(self):
        sql=f"""\
        select * from {TableList.EMS_STOCK_BILL.value} WHERE ISCURRENT = 1 AND (BILL_TYPE LIKE 'A' OR BILL_TYPE LIKE 'B') \
        AND RLT_BILL_DETAIL_SEQNO IN (SELECT BATCH_NO FROM {TableList.EMS_MANUFACTURE_TOTAL.value}) AND ORG_CODE LIKE '{self.org_code}' \
        AND WO_NO IN (SELECT WO_NO FROM {TableList.EMS_WORK_INPUT.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}')  \
        AND ACTRUAL_STOCK_DATE >= DATE'{self.startdt}' AND ACTRUAL_STOCK_DATE <= DATE'{self.enddt}' \
        """
        EMS_STOCK_BILL=Read_Oracle().read_oracle(sql=sql,database='dbods')

        sql=f"""\
        select * from {TableList.EMS_WORK_INPUT.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' AND STARTDT >= DATE'{self.startdt2}' AND STARTDT <= DATE'{self.enddt}' AND WO_NO IN (SELECT WO_NO FROM {TableList.EMS_STOCK_BILL.value}) \
        """
        EMS_WORK_INPUT=Read_Oracle().read_oracle(sql=sql,database='dbods')

        sql=f"""\
        select DISTINCT BATCH_NO,BATCH_TYPE from {TableList.EMS_MANUFACTURE_TOTAL.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' AND STARTDT >= DATE'{self.startdt2}' AND STARTDT <= DATE'{self.enddt}' AND BATCH_NO IN (SELECT RLT_BILL_DETAIL_SEQNO FROM {TableList.EMS_STOCK_BILL.value}) \
        """
        EMS_MANUFACTURE_TOTAL=Read_Oracle().read_oracle(sql=sql,database='dbods')

        # 选择出库的工单及表名 企业名领用的工单及数量
        lingyong = EMS_STOCK_BILL[EMS_STOCK_BILL.BILL_TYPE == 'A'][['QTY_CO', 'COP_G_NO', 'ORG_CODE', 'WO_NO' , 'RLT_BILL_DETAIL_SEQNO']].copy()
        # 处理多次出库汇总
        lingyong['QTY_CO'] =  lingyong['QTY_CO'].apply(lambda x:float(x))
        lingyong = lingyong.groupby(['ORG_CODE', 'WO_NO', 'COP_G_NO', 'RLT_BILL_DETAIL_SEQNO'], as_index=False)['QTY_CO'].sum()
        tuiku = EMS_STOCK_BILL[EMS_STOCK_BILL.BILL_TYPE == 'B'][['QTY_CO', 'COP_G_NO', 'ORG_CODE', 'WO_NO', 'RLT_BILL_DETAIL_SEQNO']].copy()
        # 处理多次退库汇总
        tuiku['QTY_CO'] = tuiku['QTY_CO'].apply(lambda x:float(x))
        tuiku = tuiku.groupby(['ORG_CODE', 'WO_NO', 'COP_G_NO', 'RLT_BILL_DETAIL_SEQNO'], as_index=False)['QTY_CO'].sum()
        ly_tk = pd.merge(lingyong, tuiku, on=['ORG_CODE', 'WO_NO', 'COP_G_NO', 'RLT_BILL_DETAIL_SEQNO'], how='left')
        spend_num = EMS_WORK_INPUT[['ORG_CODE', 'WO_NO', 'COP_G_NO', 'QTY_CO']].copy()
        spend_num['QTY_CO']=spend_num['QTY_CO'].fillna(0).astype(str).apply(lambda x : re.findall(r"\d+\.?\d*",x)[0])
        spend_num['QTY_CO'] = spend_num['QTY_CO'].astype(float)
        spend_num = spend_num.groupby(['ORG_CODE', 'WO_NO', 'COP_G_NO'], as_index=False)['QTY_CO'].sum()
        MX_PRO_GOLYYCRK = pd.merge(ly_tk, spend_num, on=['ORG_CODE', 'WO_NO', 'COP_G_NO'], how='left')
        MX_PRO_GOLYYCRK = MX_PRO_GOLYYCRK.fillna(0)
        MX_PRO_GOLYYCRK = MX_PRO_GOLYYCRK.rename(columns={'ORG_CODE': 'ORG_CODE', 'WO_NO':'WO_NO', 'COP_G_NO': 'COP_G_NO', 'QTY_CO_x':'OUTSTOCK_NUM',
                                        'QTY_CO_y':'CONSUME_NUM', 'QTY_CO':'BACKSTOCK_NUM'})
        Item2Type = EMS_MANUFACTURE_TOTAL[['BATCH_NO', 'BATCH_TYPE']].drop_duplicates()
        Item2Type['BATCH_NO'] = Item2Type['BATCH_NO'].map(lambda x: x.strip())
        Item2Type.drop_duplicates(inplace=True)
        MX_PRO_GOLYYCRK = pd.merge(MX_PRO_GOLYYCRK, Item2Type,left_on=['RLT_BILL_DETAIL_SEQNO'], right_on=['BATCH_NO'], how='inner')
        MX_PRO_GOLYYCRK['OUTSTOCK_NUM'] = MX_PRO_GOLYYCRK['OUTSTOCK_NUM'].map(float)
        MX_PRO_GOLYYCRK['CONSUME_NUM'] = MX_PRO_GOLYYCRK['CONSUME_NUM'].map(float)
        MX_PRO_GOLYYCRK['BACKSTOCK_NUM'] = MX_PRO_GOLYYCRK['BACKSTOCK_NUM'].map(float)
        MX_PRO_GOLYYCRK['RATIO'] = MX_PRO_GOLYYCRK['OUTSTOCK_NUM'] - MX_PRO_GOLYYCRK['CONSUME_NUM'] - MX_PRO_GOLYYCRK['BACKSTOCK_NUM']

        df_result = MX_PRO_GOLYYCRK[['ORG_CODE', 'WO_NO', 'COP_G_NO', 'RLT_BILL_DETAIL_SEQNO',
               'OUTSTOCK_NUM', 'CONSUME_NUM', 'BACKSTOCK_NUM','BATCH_TYPE','RATIO']].copy()

        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result['CHECK_TIME'] = now
        df_result['CHECK_TIME'] = pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')

        # 格式确认，添加ID列
        df_result['ORG_CODE'] = df_result['ORG_CODE'].astype(str)
        df_result['WO_NO'] = df_result['WO_NO'].astype(str)
        df_result['RLT_BILL_DETAIL_SEQNO'] = df_result['RLT_BILL_DETAIL_SEQNO'].astype(str)
        df_result['COP_G_NO'] = df_result['COP_G_NO'].astype(str)
        df_result = df_result.reset_index().rename(columns = {'index':'ID'})
        df_result['OUTSTOCK_NUM'] = df_result['OUTSTOCK_NUM'].astype(float)
        df_result['CONSUME_NUM'] = df_result['CONSUME_NUM'].astype(float)
        df_result['BACKSTOCK_NUM'] = df_result['BACKSTOCK_NUM'].astype(float)
        df_result['CHECK_TIME'] = df_result['CHECK_TIME'].astype('datetime64')
        df_result['BATCH_TYPE'] = df_result['BATCH_TYPE'].astype(str)
        df_result['RATIO'] = df_result['RATIO'].astype(float)
        df_result['CUSTOMER_CODE'] = 'FTA_LG'

        Write_Oracle().write_oracle(f'{TableList.BD_RISK_DETAIL_PRODUCTION_PD3.value}',df_result,org_code=self.org_code,alarm=None)

    def run_clean_pd3(self):
        try:
            self.clean_pd3()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '0001_0005'
    org_code, param_json, basetime = read_log_table(child_task_id)
    ProductionCleanPd3(org_code, params=param_json, base_time=basetime, child_task_id=child_task_id).run_clean_pd3()
