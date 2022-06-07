import sys, os
from os import path

sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_WAREHOUSE_STOCK_BILL, _name_BD_RISK_DETAIL_STOCK_ST3


class StockCleanSt3(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'STOCK'
        self.child_model_code = 'ST3'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.params = json.loads(params)
        self.startdt = (dt_method.strptime(base_time, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=730)) \
            .strftime('%Y-%m-%d')
        self.enddt = (dt_method.strptime(base_time, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=31)) \
            .strftime('%Y-%m-%d')

    def clean_st3(self):
        sql = """select ORG_CODE, QTY, G_UNIT, ACTRUAL_STOCK_DATE,TRADE_TOTAL, BILL_TYPE,NET_WT from {} 
        where CAPXACTION != 'D'
        and  BILL_TYPE in ('1','2', '3', '4', '5', '6', 'A', 'B') 
        and ACTRUAL_STOCK_DATE >= date'{}' and ACTRUAL_STOCK_DATE<= date'{}' 
        and org_code ='{}' and qty is not null """.format(_name_WAREHOUSE_STOCK_BILL, self.startdt, self.enddt, self.org_code)
        EMS_STOCK_BILL = Read_Oracle().read_oracle(sql=sql, database='dbods')
        df = EMS_STOCK_BILL.reset_index()

        def transfer(df, columns_list, dtype):
            for col in columns_list:
                df['{}'.format(col)] = df['{}'.format(col)].astype(dtype)

        transfer(df, ['QTY', 'TRADE_TOTAL', 'NET_WT'], float)
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df['CHECK_TIME'] = now
        df['CHECK_TIME'] = df['CHECK_TIME'].astype('datetime64')
        BD_RISK_DETAIL_STOCK_ST3 = df.drop('index', axis=1).reset_index().rename(columns={'index': 'ID','QTY':'QTY_CO','G_UNIT':'UNIT_CO'})
        BD_RISK_DETAIL_STOCK_ST3['QTY'] = 0

        # 明确企业所属租户
        BD_RISK_DETAIL_STOCK_ST3['CUSTOMER_CODE'] = 'FTA_LG'

        Write_Oracle().write_oracle(_name_BD_RISK_DETAIL_STOCK_ST3, BD_RISK_DETAIL_STOCK_ST3, org_code=self.org_code,
                                    alarm=None)

    def run_clean_st3(self):
        try:
            self.clean_st3()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    child_task_id = sys.argv[1]
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    # child_task_id = '6d1f6122e0f54dfa829cc2d92be763d1'
    org_code, param_json, base_time = read_log_table(child_task_id)
    StockCleanSt3(org_code=org_code, params=param_json, base_time=base_time,
                  child_task_id=child_task_id).run_clean_st3()
