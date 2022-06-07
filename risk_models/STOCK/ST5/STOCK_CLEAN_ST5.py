import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class StockCleanSt5(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'STOCK'
        self.child_model_code = 'ST5'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.startdt = datetime.datetime.strptime(base_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=-730)
        self.startdt = self.startdt.strftime('%Y-%m-%d')
        self.enddt = datetime.datetime.strptime(base_time, "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')

    def clean_st5(self):
        # 取数
        sql = f"""\
        select * from {TableList.WAREHOUSE_STOCK_BILL.value} WHERE ACTRUAL_STOCK_DATE >= DATE'{self.startdt}' AND ACTRUAL_STOCK_DATE <= DATE'{self.enddt}' AND ORG_CODE LIKE '{self.org_code}' AND ISCURRENT = 1 \
        AND STOCK_BILL_TYPE = '1' AND ORDER_NO IS NOT NULL
        """
        EMS_STOCK_BILL = Read_Oracle().read_oracle(sql=sql, database='dbods')
        
        sql = f"""\
        select * from {TableList.EMS_DELIV_DETAIL.value} WHERE ORG_CODE LIKE '{self.org_code}' 
        """
        EMS_DELIV_DETAIL = Read_Oracle().read_oracle(sql=sql, database='dbods')

        # 应加入公司ID筛选，默认不取CAPXACTION为D的数据
        sql = f"""
        select * from {TableList.EMS_ORDER_DETAIL.value} WHERE ORG_CODE LIKE '{self.org_code}' \
        """
        EMS_ORDER_DETAIL = Read_Oracle().read_oracle(sql=sql, database='dbods')

        # 应加入公司ID筛选，默认不取CAPXACTION为D的数据
        sql = f"""
        select * from {TableList.EMS_ORDER_HEAD.value} WHERE ORG_CODE LIKE '{self.org_code}'  \
        """
        EMS_ORDER_HEAD = Read_Oracle().read_oracle(sql=sql, database='dbods')

        # 以下为数据类型转换的，正式部署应删除

        # EMS_STOCK_BILL['QTY']=EMS_STOCK_BILL['QTY'].map(float)
        EMS_STOCK_BILL['QTY_CO'] = EMS_STOCK_BILL['QTY_CO'].map(float)
        # EMS_STOCK_BILL['STOCK_BILL_TYPE']=EMS_STOCK_BILL['STOCK_BILL_TYPE'].map(float)
        EMS_STOCK_BILL['TRADE_TOTAL'] = EMS_STOCK_BILL['TRADE_TOTAL'].fillna(0).map(float)
        # 用N/A填充ORDER_NO为空的列，正式部署时应删除
        EMS_STOCK_BILL['ORDER_NO'] = EMS_STOCK_BILL['ORDER_NO'].fillna('N/A').map(str)
        EMS_DELIV_DETAIL['QTY_DELIV']=EMS_DELIV_DETAIL['QTY_DELIV'].map(float)
        EMS_ORDER_DETAIL['TRADE_TOTAL'] = EMS_ORDER_DETAIL['TRADE_TOTAL'].fillna(0).map(float)

        # 计算入库数量比例
        def ST5_RATE_CAL(row):
            if row['DELIV_FLAG'] == 1:
                if ((row['QTY_DELIV'] == 0)|(pd.isnull(row['QTY_DELIV']))):
                    qty_deliv = 0.01
                else:
                    qty_deliv = row['QTY_DELIV']
                rate = (row['QTY_STOCK'] - row['QTY_DELIV']) / qty_deliv
            else:
                rate = None
            return rate

        # 计算时间差用的，时间差
        def ST5_DATE_DIFF(row):
            datediff = row['CHECK_TIME'] - row['SIGN_DT']
            datediff = datediff.days
            return datediff

        # 取入库的，且有订单号的数据
        t1_temp = EMS_STOCK_BILL[(EMS_STOCK_BILL['STOCK_BILL_TYPE'] == '1') & (-pd.isnull(EMS_STOCK_BILL['ORDER_NO']))][
            ['ORG_CODE', 'ORG_NAME', 'COP_G_NO', 'ORDER_NO', 'QTY', 'QTY_CO', 'TRADE_TOTAL']].copy()

        # 根据ORG_CODE+ORDER_NO+料号统计入库的数量，入库数量最好使用 QTY（法定数量）
        # t1 : 入库的数据
        t1 = t1_temp.groupby(['ORG_CODE', 'ORDER_NO', 'COP_G_NO']).agg({'QTY_CO': 'sum', 'TRADE_TOTAL': 'sum'})

        # 利用公司代码+订单头表ID将头表与明细表关联起来
        t2 = pd.merge(EMS_ORDER_HEAD[['ORG_CODE', 'ORDER_NO', 'SIGN_DT', 'ID']],
                      EMS_ORDER_DETAIL[['ORG_CODE', 'ID', 'ORDER_ID', 'COP_G_NO', 'QTY_ORDER', 'TRADE_TOTAL']],
                      how='inner', left_on=['ORG_CODE', 'ID'], right_on=['ORG_CODE', 'ORDER_ID'])

        # 删掉订单ID，将订单明细表的ID改为ORDER_DETAIL_ID
        t2 = t2.drop(columns=['ID_x', 'ORDER_ID'])
        t2 = t2.rename(columns={'ID_y': 'ORDER_DETAIL_ID', 'TRADE_TOTAL': 'PRICE_ORDER'})

        # 用公司代码+料号+订单明细ID，将订单数据与订单交货数据进行左关联
        # t2 : 采购订单的交货数据
        t2 = pd.merge(t2, EMS_DELIV_DETAIL[['ORG_CODE', 'ORDER_DETAIL_ID', 'COP_G_NO', 'QTY_DELIV', 'TRADE_TOTAL']],
                      how='left', left_on=['ORG_CODE', 'COP_G_NO', 'ORDER_DETAIL_ID'],
                      right_on=['ORG_CODE', 'COP_G_NO', 'ORDER_DETAIL_ID'])

        t2 = t2.drop(columns=['ORDER_DETAIL_ID'])
        t2 = t2.rename(columns={'TRADE_TOTAL': 'PRICE_DELIV'})

        # 根据公司+订单号+料号对入库数据与订单数据进行右连接
        t3 = pd.merge(t1, t2, how='right', left_on=['ORG_CODE', 'ORDER_NO', 'COP_G_NO'],
                      right_on=['ORG_CODE', 'ORDER_NO', 'COP_G_NO'])

        t3 = t3.rename(columns={'QTY_CO': 'QTY_STOCK', 'TRADE_TOTAL': 'PRICE_STOCK'})

        # 开始进行数据处理
        # 默认DELIV_FLAG为1，然后挑出交付数量为空的，FLAG改写为0
        t3['DELIV_FLAG'] = 1
        t3.loc[pd.isnull(t3['QTY_DELIV']), 'DELIV_FLAG'] = 0

        # 计算RATE
        t3['RATE'] = t3.apply(ST5_RATE_CAL, axis=1)

        # 计算
        t3['SIGN_DT'] = t3['SIGN_DT'].apply(lambda x : x.date())
        t3['CHECK_TIME'] = datetime.date.today()
        t3['DAY_DIFF'] = t3.apply(ST5_DATE_DIFF, axis=1)

        # 处理
        t3['QTY_STOCK'] = t3['QTY_STOCK'].fillna(0)

        # 格式确认，添加ID列
        t3['ORG_CODE'] = t3['ORG_CODE'].astype(str)
        t3['ORDER_NO'] = t3['ORDER_NO'].astype(str)
        t3['COP_G_NO'] = t3['COP_G_NO'].astype(str)
        t3['DELIV_FLAG'] = t3['DELIV_FLAG'].astype(int)
        t3 = t3.reset_index().rename(columns={'index': 'ID'})
        t3['CHECK_TIME'] = t3['CHECK_TIME'].astype('datetime64')
        t3['SIGN_DT'] = t3['SIGN_DT'].astype('datetime64')
        t3['QTY_STOCK'] = t3['QTY_STOCK'].astype(float)
        t3['QTY_DELIV'] = t3['QTY_DELIV'].astype(float)
        t3['QTY_ORDER'] = t3['QTY_ORDER'].astype(float)
        t3['PRICE_STOCK'] = t3['PRICE_STOCK'].astype(float)
        t3['PRICE_DELIV'] = t3['PRICE_DELIV'].astype(float)
        t3['PRICE_ORDER'] = t3['PRICE_ORDER'].astype(float)
        t3['RATE'] = t3['RATE'].astype(float)
        t3['CUSTOMER_CODE'] = 'FTA_LG'

        # 写入
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_DETAIL_STOCK_ST5.value}', t3, org_code=self.org_code,alarm=None)

    def run_clean_st5(self):
        try:
            self.clean_st5()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    child_task_id = sys.argv[1]
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    # child_task_id = '0001_0005'
    org_code, param_json, base_time = read_log_table(child_task_id)
    StockCleanSt5(org_code=org_code, params=param_json, base_time=base_time,
                  child_task_id=child_task_id).run_clean_st5()