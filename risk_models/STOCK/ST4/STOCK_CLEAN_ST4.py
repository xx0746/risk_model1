import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class StockCleanSt4(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'STOCK'
        self.child_model_code = 'ST4'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.startdt = datetime.datetime.strptime(base_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-730)
        self.startdt = self.startdt.strftime('%Y-%m-%d')
        self.enddt = datetime.datetime.strptime(base_time, "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')

    def clean_st4(self):

        # 计算PRICE_DELIV_EST用的，PRICE_DELIV_EST = QTY_DELIV * PRICE_ORDER / QTY_ORDER
        def ST4_EST_PRICE_CAL(row):
            if pd.isnull(row['QTY_ORDER']):
                QTY_ORDER = 0.001
                PRICE_DELIV_EST = 0.001
            else:
                PRICE_DELIV_EST = row['QTY_DELIV'] * row['PRICE_ORDER'] / row['QTY_ORDER']
                if PRICE_DELIV_EST == 0:
                    PRICE_DELIV_EST = 0.001
            return PRICE_DELIV_EST

        # 计算RATE用的，RATE = (入-交付)/交付
        def ST4_RATE_CAL(row):
            if pd.isnull(row['PRICE_STOCK']):
                PRICE_STOCK = 0
            else:
                PRICE_STOCK = row['PRICE_STOCK']
            if pd.isnull(row['PRICE_DELIV_EST']):
                PRICE_DELIV_EST = 0.01
            else:
                PRICE_DELIV_EST = row['PRICE_DELIV_EST']
            rate = (PRICE_STOCK - PRICE_DELIV_EST) / PRICE_DELIV_EST
            return rate

        # 计算时间差用的，时间差
        def ST4_DATE_DIFF(row):
            datediff = row['CHECK_TIME'] - row['SIGN_DT']
            datediff = datediff.days
            return datediff

        # 读数
        sql = f"""\
        select * from {TableList.BD_RISK_DETAIL_STOCK_ST5.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code}' AND DELIV_FLAG = 1 and CUSTOMER_CODE = 'FTA_LG'\
        """
        ST4_DATA = Read_Oracle().read_oracle(sql=sql, database='dbdm')

        df_result = ST4_DATA[['ORG_CODE', 'ORDER_NO', 'COP_G_NO', 'SIGN_DT', \
                              'QTY_ORDER', 'QTY_DELIV', 'QTY_STOCK', 'PRICE_ORDER', 'PRICE_DELIV', \
                              'PRICE_STOCK', 'CHECK_TIME', 'ISCURRENT', 'LASTUPDATE']].copy()

        # 计算RATE
        df_result['PRICE_DELIV_EST'] = df_result.apply(ST4_EST_PRICE_CAL, axis=1)
        df_result['RATE'] = df_result.apply(ST4_RATE_CAL, axis=1)

        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result['CHECK_TIME'] = now
        df_result['CHECK_TIME'] = pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')

        # 计算
        df_result['DAY_DIFF'] = df_result.apply(ST4_DATE_DIFF, axis=1)

        # 格式确认，添加ID列
        df_result = df_result.reset_index().rename(columns={'index': 'ID'})
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
        df_result['PRICE_DELIV_EST'] = df_result['PRICE_DELIV_EST'].astype(float)
        df_result['RATE'] = df_result['RATE'].astype(float)
        df_result['CUSTOMER_CODE'] = 'FTA_LG'

        # 写入
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_DETAIL_STOCK_ST4.value}', df_result, org_code=self.org_code,
                                    alarm=None)

    def run_clean_st4(self):
        try:
            self.clean_st4()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    child_task_id = sys.argv[1]
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    # child_task_id = 'a52f4dc487f34cf8ab94de15fecfa880'
    org_code, param_json, base_time = read_log_table(child_task_id)
    StockCleanSt4(org_code, params=param_json, base_time=base_time, child_task_id=child_task_id).run_clean_st4()
