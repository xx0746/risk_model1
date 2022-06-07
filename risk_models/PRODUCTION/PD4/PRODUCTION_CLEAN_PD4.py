import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class ProductionCleanPd4(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'Production'
        self.child_model_code = 'PD4'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.startdt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-90)
        self.startdt = self.startdt.strftime('%Y-%m-%d')
        self.startdt2 = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-121)
        self.startdt2 = self.startdt2.strftime('%Y-%m-%d')
        self.enddt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')
        


    def clean_pd4(self):
        sql=f"""\
        select BILL_TYPE,ACTRUAL_STOCK_DATE,COP_G_NO,ORG_CODE,WO_NO,RLT_BILL_DETAIL_SEQNO from {TableList.EMS_STOCK_BILL.value} WHERE ISCURRENT = 1 AND ACTRUAL_STOCK_DATE >= DATE'{self.startdt}' AND ACTRUAL_STOCK_DATE <= DATE'{self.enddt}' AND ORG_CODE LIKE '{self.org_code}' AND (BILL_TYPE LIKE 'A' or BILL_TYPE LIKE 'B')\
        """
        EMS_STOCK_BILL=Read_Oracle().read_oracle(sql=sql,database='dbods')

        # 选择出库的工单及表名 企业名领用的工单及数量
        lingyong = EMS_STOCK_BILL[EMS_STOCK_BILL.BILL_TYPE == 'A'][['ACTRUAL_STOCK_DATE', 'COP_G_NO', 'ORG_CODE', 'WO_NO' , 'RLT_BILL_DETAIL_SEQNO']].copy()
        lingyong.dropna(subset=['RLT_BILL_DETAIL_SEQNO'],inplace=True)
        lingyong = lingyong.rename(columns={'ACTRUAL_STOCK_DATE': 'OUT_TIME'})
        tuiku = EMS_STOCK_BILL[EMS_STOCK_BILL.BILL_TYPE == 'B'][['ACTRUAL_STOCK_DATE' , 'COP_G_NO', 'ORG_CODE', 'WO_NO', 'RLT_BILL_DETAIL_SEQNO']].copy()
        tuiku.dropna(subset=['RLT_BILL_DETAIL_SEQNO'],inplace=True)
        EMS_STOCK_BILL = None
        tuiku = tuiku.rename(columns={'ACTRUAL_STOCK_DATE': 'BACK_TIME'})

        # 存在一票工单下的多种料号的退库时间，后续得排除这方面的因素（目前采用早得退库时间【企业、工单】）
        ly_tk = pd.merge(lingyong, tuiku, on=['ORG_CODE', 'WO_NO', 'COP_G_NO', 'RLT_BILL_DETAIL_SEQNO'], how='left')
        lingyong = None
        tuiku = None

        sql=f"""\
        select COP_G_NO,ORG_CODE,WO_NO from {TableList.EMS_WORK_INPUT.value} WHERE ISCURRENT = 1 AND STARTDT >= DATE'{self.startdt2}' AND STARTDT <= DATE'{self.enddt}' AND ORG_CODE LIKE '{self.org_code}'  \
        """
        EMS_WORK_INPUT=Read_Oracle().read_oracle(sql=sql,database='dbods')

        sql=f"""\
        select WO_NO,LASTUPDATEDDT,BATCH_NO,ORG_CODE from {TableList.EMS_WORK_OUTPUT.value} WHERE ISCURRENT = 1 AND LASTUPDATEDDT >= DATE'{self.startdt2}' AND LASTUPDATEDDT <= DATE'{self.enddt}' AND ORG_CODE LIKE '{self.org_code}'  \
        """
        EMS_WORK_OUTPUT=Read_Oracle().read_oracle(sql=sql,database='dbods')

        sql=f"""\
        select ORG_CODE,WO_DATE,WO_NO from {TableList.EMS_WORK_HEAD.value} WHERE ISCURRENT = 1 AND WO_DATE >= DATE'{self.startdt2}' AND WO_DATE <= DATE'{self.enddt}' and WO_TYPE = 1 AND ORG_CODE LIKE '{self.org_code}' \
        AND WO_NO IN (SELECT WO_NO FROM {TableList.EMS_WORK_INPUT.value})  \
        """
        EMS_WORK_HEAD=Read_Oracle().read_oracle(sql=sql,database='dbods')

        # 获取耗用清单的批次号
        batch_no = EMS_WORK_OUTPUT[['ORG_CODE', 'WO_NO', 'BATCH_NO']].copy()
        batch_no = batch_no.drop_duplicates()
        EMS_WORK_INPUT_bno = pd.merge(EMS_WORK_INPUT, batch_no, left_on=['ORG_CODE', 'WO_NO'], 
                                  right_on=['ORG_CODE', 'WO_NO'], how='left')

        EMS_WORK_INPUT_bno = EMS_WORK_INPUT_bno[['COP_G_NO', 'ORG_CODE', 'WO_NO', 'BATCH_NO']].copy()
        EMS_WORK_INPUT_bno = EMS_WORK_INPUT_bno.drop_duplicates()
        EMS_WORK_INPUT = None
        # 从耗料清单的表头中获取耗料清单的创建时间，料号是INPUT里的料号
        EMS_WORK_HEAD_time = pd.merge(EMS_WORK_HEAD[['ORG_CODE', 'WO_DATE', 'WO_NO']], EMS_WORK_INPUT_bno, 
                              on=['ORG_CODE', 'WO_NO'], how='left')
        EMS_WORK_HEAD = None
        MX_PRO_GDLLHLXFX = pd.merge(ly_tk ,EMS_WORK_HEAD_time, left_on = ['COP_G_NO', 'ORG_CODE', 'WO_NO', 'RLT_BILL_DETAIL_SEQNO'],
                            right_on=['COP_G_NO', 'ORG_CODE', 'WO_NO', 'BATCH_NO'], how='left')
        ly_tk = None
        MX_PRO_GDLLHLXFX = MX_PRO_GDLLHLXFX.rename(columns={'WO_DATE':'WO_TIME'})
        EMS_WORK_OUTPUT = EMS_WORK_OUTPUT[['WO_NO', 'LASTUPDATEDDT', 'BATCH_NO', 'ORG_CODE']].copy()
        MX_PRO_GDLLHLXFX['COP_G_NO'] = MX_PRO_GDLLHLXFX['COP_G_NO'].map(str)
        MX_PRO_GDLLHLXFX = pd.merge(MX_PRO_GDLLHLXFX, EMS_WORK_OUTPUT, left_on=['ORG_CODE','WO_NO','BATCH_NO'],
                           right_on=['ORG_CODE', 'WO_NO','BATCH_NO'], how='left')
        EMS_WORK_OUTPUT = None
        MX_PRO_GDLLHLXFX = MX_PRO_GDLLHLXFX.rename(columns={'LASTUPDATEDDT':'INSTOCK_TIME'})
        df_result = MX_PRO_GDLLHLXFX[['ORG_CODE','WO_NO','RLT_BILL_DETAIL_SEQNO','COP_G_NO','OUT_TIME','BACK_TIME','WO_TIME','INSTOCK_TIME']].copy()
        MX_PRO_GDLLHLXFX = None
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result['CHECK_TIME'] = now
        df_result['CHECK_TIME'] = pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')

        # 格式确认，添加ID列
        df_result['ORG_CODE'] = df_result['ORG_CODE'].astype(str)
        df_result['WO_NO'] = df_result['WO_NO'].astype(str)
        df_result['RLT_BILL_DETAIL_SEQNO'] = df_result['RLT_BILL_DETAIL_SEQNO'].astype(str)
        df_result['COP_G_NO'] = df_result['COP_G_NO'].astype(str)
        df_result = df_result.reset_index().rename(columns = {'index':'ID'})
        df_result['OUT_TIME'] = df_result['OUT_TIME'].astype('datetime64')
        df_result['BACK_TIME'] = df_result['BACK_TIME'].astype('datetime64')
        df_result['WO_TIME'] = df_result['WO_TIME'].astype('datetime64')
        df_result['INSTOCK_TIME'] = df_result['INSTOCK_TIME'].astype('datetime64')
        df_result['CHECK_TIME'] = df_result['CHECK_TIME'].astype('datetime64')
        df_result['CUSTOMER_CODE'] = 'FTA_LG'

        Write_Oracle().write_oracle(f'{TableList.BD_RISK_DETAIL_PRODUCTION_PD4.value}',df_result,org_code=self.org_code,alarm=None)

    def run_clean_pd4(self):
        try:
            self.clean_pd4()
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
    ProductionCleanPd4(org_code, params=param_json, base_time=basetime, child_task_id = child_task_id).run_clean_pd4()
