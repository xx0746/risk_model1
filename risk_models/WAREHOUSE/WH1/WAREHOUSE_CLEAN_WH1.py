import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class WarehouseCleanWh1(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'WAREHOUSE'
        self.child_model_code = 'WH1'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        # self.startdt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-730)
        # self.startdt2 = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-760)
        self.startdt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-450)
        self.startdt2 = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-480)
        self.startdt = self.startdt.strftime('%Y-%m-%d')
        self.startdt2 = self.startdt2.strftime('%Y-%m-%d')
        self.enddt = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')       


    def clean_wh1(self):
        # 为了测试有限制行数，记得去掉行数限制
        sql = f"""\
        select * from {TableList.WAREHOUSE_STOCK_BILL.value} where org_code like '{self.org_code}' and iscurrent = 1 and dclcus_flag = 1 AND STARTDT >= DATE'{self.startdt}' AND STARTDT <= DATE'{self.enddt}'
        """
        STOCK_BILL = Read_Oracle().read_oracle(sql=sql, database='dbods')

        # 报关单语句应加上时间限制，用 startdt2
        sql = f"""\
        SELECT ID,I_E_MARK,CUSTOMS_CODE,I_E_PORT,I_E_DATE,FIRST_DECL_TIME,TRAF_MODE,GOODS_OWNER FROM {TableList.FT_I_DTL_SEA_PRE_RECORDED.value} WHERE ISDELETED = 0 AND TRADE_CODE_SCC LIKE '{self.org_code}' AND I_E_DATE >= DATE'{self.startdt2}' AND I_E_DATE <= DATE'{self.enddt}'
        """
        CUS_SEA_HEAD_I = Read_Oracle().read_oracle(sql=sql, database='dbdw')

        sql = f"""\
        SELECT HEAD_ID, ENTRY_NO, GOODS_NO, GOODS_CODE, GOODS_NAME, GOODS_MODEL, 
        ORG_CODE_CY, G_QTY, G_UNIT, QTY_1, UNIT_1, QTY_2, UNIT_2, TRADE_CURR_CODE, 
        DECL_TOTAL,'SEA' AS SRC FROM {TableList.FT_I_DTL_SEA_LIST.value} WHERE ISDELETED = 0
        AND HEAD_ID IN
        (SELECT ID FROM {TableList.FT_I_DTL_SEA_PRE_RECORDED.value} WHERE ISDELETED = 0 AND TRADE_CODE_SCC LIKE '{self.org_code}' AND I_E_DATE >= DATE'{self.startdt2}' AND I_E_DATE <= DATE'{self.enddt}')
        """
        CUS_SEA_LIST_I = Read_Oracle().read_oracle(sql=sql, database='dbdw')

        sql = f"""\
        SELECT ID,I_E_MARK,CUSTOMS_CODE,I_E_PORT,I_E_DATE,FIRST_DECL_TIME,TRAF_MODE,GOODS_OWNER FROM {TableList.FT_I_DTL_OTR_PRE_RECORDED.value} WHERE ISDELETED = 0 AND TRADE_CODE_SCC LIKE '{self.org_code}' AND I_E_DATE >= DATE'{self.startdt2}' AND I_E_DATE <= DATE'{self.enddt}'
        """
        CUS_OTR_HEAD_I = Read_Oracle().read_oracle(sql=sql, database='dbdw')

        sql = f"""\
        SELECT HEAD_ID, ENTRY_NO, GOODS_NO, GOODS_CODE, GOODS_NAME, GOODS_MODEL, 
        ORG_CODE_CY, G_QTY, G_UNIT, QTY_1, UNIT_1, QTY_2, UNIT_2, TRADE_CURR_CODE, 
        DECL_TOTAL,'OTR' AS SRC FROM {TableList.FT_I_DTL_OTR_LIST.value} WHERE ISDELETED = 0
        AND HEAD_ID IN
        (SELECT ID FROM {TableList.FT_I_DTL_OTR_PRE_RECORDED.value} WHERE ISDELETED = 0 AND TRADE_CODE_SCC LIKE '{self.org_code}' AND I_E_DATE >= DATE'{self.startdt2}' AND I_E_DATE <= DATE'{self.enddt}')
        """
        CUS_OTR_LIST_I = Read_Oracle().read_oracle(sql=sql, database='dbdw')

        sql = f"""\
        SELECT ID,I_E_MARK,CUSTOMS_CODE,I_E_PORT,I_E_DATE,FIRST_DECL_TIME,TRAF_MODE,GOODS_OWNER FROM {TableList.FT_E_DTL_SEA_PRE_RECORDED.value} WHERE ISDELETED = 0 AND TRADE_CODE_SCC LIKE '{self.org_code}' AND I_E_DATE >= DATE'{self.startdt2}' AND I_E_DATE <= DATE'{self.enddt}'
        """
        CUS_SEA_HEAD_E = Read_Oracle().read_oracle(sql=sql, database='dbdw')

        sql = f"""\
        SELECT HEAD_ID, ENTRY_NO, GOODS_NO, GOODS_CODE, GOODS_NAME, GOODS_MODEL, 
        ORG_CODE_CY, G_QTY, G_UNIT, QTY_1, UNIT_1, QTY_2, UNIT_2, TRADE_CURR_CODE, 
        DECL_TOTAL,'SEA' AS SRC FROM {TableList.FT_E_DTL_SEA_LIST.value} WHERE ISDELETED = 0
        AND HEAD_ID IN
        (SELECT ID FROM {TableList.FT_E_DTL_SEA_PRE_RECORDED.value} WHERE ISDELETED = 0 AND TRADE_CODE_SCC LIKE '{self.org_code}' AND I_E_DATE >= DATE'{self.startdt2}' AND I_E_DATE <= DATE'{self.enddt}')
        """
        CUS_SEA_LIST_E = Read_Oracle().read_oracle(sql=sql, database='dbdw')

        sql = f"""\
        SELECT ID,I_E_MARK,CUSTOMS_CODE,I_E_PORT,I_E_DATE,FIRST_DECL_TIME,TRAF_MODE,GOODS_OWNER FROM {TableList.FT_E_DTL_OTR_PRE_RECORDED.value} WHERE ISDELETED = 0 AND TRADE_CODE_SCC LIKE '{self.org_code}' AND I_E_DATE >= DATE'{self.startdt2}' AND I_E_DATE <= DATE'{self.enddt}'
        """
        CUS_OTR_HEAD_E = Read_Oracle().read_oracle(sql=sql, database='dbdw')

        sql = f"""\
        SELECT HEAD_ID, ENTRY_NO, GOODS_NO, GOODS_CODE, GOODS_NAME, GOODS_MODEL, 
        ORG_CODE_CY, G_QTY, G_UNIT, QTY_1, UNIT_1, QTY_2, UNIT_2, TRADE_CURR_CODE, 
        DECL_TOTAL,'OTR' AS SRC FROM {TableList.FT_E_DTL_OTR_LIST.value} WHERE ISDELETED = 0
        AND HEAD_ID IN
        (SELECT ID FROM {TableList.FT_E_DTL_OTR_PRE_RECORDED.value} WHERE ISDELETED = 0 AND TRADE_CODE_SCC LIKE '{self.org_code}' AND I_E_DATE >= DATE'{self.startdt2}' AND I_E_DATE <= DATE'{self.enddt}')
        """
        CUS_OTR_LIST_E = Read_Oracle().read_oracle(sql=sql, database='dbdw')

        sql = f"""\
        SELECT CY_CODE_ALPHY3,CY_CODE_CUS_NUMBER FROM {TableList.DIM_COUNTRY.value} WHERE ISCURRENT = 1 AND REGEXP_LIKE(CY_CODE_CUS_NUMBER,'^[0-9]*$') AND CY_CODE_CUS_NUMBER NOT LIKE '299' 
        """
        DIM_COUNTRY = Read_Oracle().read_oracle(sql=sql, database='dbdw')

        sql = f"""\
        SELECT CUS_CURRENCY_CODE,CURRENCY_CODE FROM {TableList.MAP_GJ_CURRENCY.value}
        """
        DIM_CURR = Read_Oracle().read_oracle(sql=sql, database='dbdw')

        CUS_SEA_IMPORT = pd.merge(CUS_SEA_HEAD_I,CUS_SEA_LIST_I,how='inner',left_on=['ID'],right_on=['HEAD_ID'])
        CUS_SEA_IMPORT = CUS_SEA_IMPORT.drop_duplicates(subset=['ENTRY_NO','GOODS_NO'])

        CUS_OTR_IMPORT = pd.merge(CUS_OTR_HEAD_I,CUS_OTR_LIST_I,how='inner',left_on=['ID'],right_on=['HEAD_ID'])
        CUS_OTR_IMPORT = CUS_OTR_IMPORT.drop_duplicates(subset=['ENTRY_NO','GOODS_NO'])

        CUS_SEA_EXPORT = pd.merge(CUS_SEA_HEAD_E,CUS_SEA_LIST_E,how='inner',left_on=['ID'],right_on=['HEAD_ID'])
        CUS_SEA_EXPORT = CUS_SEA_EXPORT.drop_duplicates(subset=['ENTRY_NO','GOODS_NO'])

        CUS_OTR_EXPORT = pd.merge(CUS_OTR_HEAD_E,CUS_OTR_LIST_E,how='inner',left_on=['ID'],right_on=['HEAD_ID'])
        CUS_OTR_EXPORT = CUS_OTR_EXPORT.drop_duplicates(subset=['ENTRY_NO','GOODS_NO'])

        CUS_DATA = pd.concat([CUS_SEA_IMPORT,CUS_OTR_IMPORT,CUS_SEA_EXPORT,CUS_OTR_EXPORT])

        CUS_DATA['TRADE_CURR_CODE'].replace(to_replace = list(DIM_CURR['CURRENCY_CODE']),value = list(DIM_CURR['CUS_CURRENCY_CODE']),inplace=True)

        CUS_DATA['ORG_CODE_CY'].replace(to_replace = list(DIM_COUNTRY['CY_CODE_ALPHY3']),value = list(DIM_COUNTRY['CY_CODE_CUS_NUMBER']),inplace=True)
        CUS_DATA = CUS_DATA.drop(columns=['ID','HEAD_ID'])
        CUS_DATA = CUS_DATA.reset_index().rename(columns = {'index':'ID'})
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        CUS_DATA['CHECK_TIME']=now
        CUS_DATA['CHECK_TIME']= pd.to_datetime(CUS_DATA['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        CUS_DATA['ORG_CODE'] = self.org_code

        STOCK_BILL['TRADE_TOTAL'] = STOCK_BILL['TRADE_TOTAL'].astype(float)
        STOCK_BILL['QTY'] = STOCK_BILL['QTY'].astype(float)
        STOCK_BILL['QTY_1'] = STOCK_BILL['QTY_1'].astype(float)
        STOCK_BILL['QTY_2'] = STOCK_BILL['QTY_2'].astype(float)

        stock_bill_sum = STOCK_BILL.groupby(['ORG_CODE','ENTRY_NO','ENTRY_GDS_SEQNO'])['QTY','QTY_1','QTY_2','TRADE_TOTAL'].sum()
        stock_bill_sum = stock_bill_sum.reset_index()
        stock_info = STOCK_BILL[['ORG_CODE','ENTRY_NO','ENTRY_GDS_SEQNO',
                                        'STOCK_BILL_TYPE','STOCK_TYPE','BILL_TYPE',
                                        'BUSINESS_TYPE','BILL_STATUS','CLASSIFY_TYPE',
                                        'SUPV_MODE','TRAF_MODE','I_E_PORT','CUSTOMS_CODE',
                                        'G_NAME','G_MODEL','ORIGIN_COUNTRY_CODE','HS_CODE','G_UNIT','UNIT_1',
                                        'UNIT_2','TRADE_CURR','STARTDT']].copy()

        stock_info = stock_info.drop_duplicates(subset=['ORG_CODE','ENTRY_NO','ENTRY_GDS_SEQNO'])
        stock_bill_clean = pd.merge(stock_bill_sum,stock_info,how='left',left_on=['ORG_CODE','ENTRY_NO','ENTRY_GDS_SEQNO'],
                        right_on=['ORG_CODE','ENTRY_NO','ENTRY_GDS_SEQNO'])
        stock_bill_clean['TRADE_CURR'].replace(to_replace=list(DIM_CURR['CURRENCY_CODE']),value=list(DIM_CURR['CUS_CURRENCY_CODE']),inplace=True)
        stock_bill_clean['ENTRY_GDS_SEQNO'] = stock_bill_clean['ENTRY_GDS_SEQNO'].astype(int)

        stock_bill_clean = stock_bill_clean.reset_index().rename(columns = {'index':'ID'})
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        stock_bill_clean['CHECK_TIME']=now
        stock_bill_clean['CHECK_TIME']= pd.to_datetime(stock_bill_clean['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        stock_bill_clean['TRADE_TOTAL'] = stock_bill_clean['TRADE_TOTAL'].astype(float)
        stock_bill_clean['QTY'] = stock_bill_clean['QTY'].astype(float)
        stock_bill_clean['QTY_1'] = stock_bill_clean['QTY_1'].astype(float)
        stock_bill_clean['QTY_2'] = stock_bill_clean['QTY_2'].astype(float)
        CUS_DATA['CUSTOMER_CODE'] = 'FTA_LG'
        stock_bill_clean['CUSTOMER_CODE'] = 'FTA_LG'

        # 写入
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_DETAIL_WAREHOUSE_WH1_C.value}',CUS_DATA,org_code=self.org_code,alarm=None)
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_DETAIL_WAREHOUSE_WH1_S.value}',stock_bill_clean,org_code=self.org_code,alarm=None)

    def run_clean_wh1(self):
        try:
            self.clean_wh1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = 'childtask_wh1_1'
    org_code, param_json, base_time = read_log_table(child_task_id)
    WarehouseCleanWh1(org_code, params=param_json, base_time = base_time, child_task_id=child_task_id).run_clean_wh1()