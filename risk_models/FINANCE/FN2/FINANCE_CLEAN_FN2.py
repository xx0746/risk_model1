import sys, os
from os import path

sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_EMS_FINANCE_INFO, _name_EMS_MANUFACTURE_TOTAL


class FinanceCleanFn2(object):
    def __init__(self, org_code, child_task_id, params):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code

    def clean_fn2(self):
        # 分别读取财务明细表和加工整机耗料明细表
        EMS_FINANCE_INFO = Read_Oracle().read_oracle(sql=""" select * from {} where CAPXACTION != 'D' and ORG_CODE = '{}' 
        and KSTAR like '%工资%' """.format(_name_EMS_FINANCE_INFO, self.org_code), database='dbods')
        EMS_MANUFACTURE_TOTAL = Read_Oracle().read_oracle(sql=""" select distinct batch_no, batch_type from {} where CAPXACTION != 'D' and ORG_CODE = '{}' and batch_no in 
        (select obj_code from {} where KSTAR like '%工资%' and org_code = '{}')""".format(_name_EMS_MANUFACTURE_TOTAL,
                                                                                   self.org_code,
                                                                                   _name_EMS_FINANCE_INFO,
                                                                                   self.org_code),
                                                                                   database='dbods')

        # 清洗财务明细表
        EMS_FINANCE_INFO['TRADE_TOTAL'] = EMS_FINANCE_INFO['TRADE_TOTAL'].apply(lambda x: ''.join(x.split(',')))
        EMS_FINANCE_INFO['TRADE_TOTAL'] = EMS_FINANCE_INFO['TRADE_TOTAL'].map(float)
        EMS_FINANCE_INFO.dropna(axis=0, subset=['OBJ_CODE'], inplace=True)

        # 以OBJ_CODE/ORG_CODE为粒度进行聚合，避免一个批次有多条财务记录的情况
        EMS_FINANCE_INFO = EMS_FINANCE_INFO.groupby(['ORG_CODE', 'OBJ_CODE'])['TRADE_TOTAL'].sum().reset_index()

        # 汇总两张表格
        EMS_FINANCE_LABOR_COST = EMS_FINANCE_INFO.merge(
            EMS_MANUFACTURE_TOTAL[['BATCH_NO', 'BATCH_TYPE']].drop_duplicates(), left_on='OBJ_CODE',
            right_on='BATCH_NO', how='left')
        EMS_FINANCE_LABOR_COST = EMS_FINANCE_LABOR_COST[['ORG_CODE', 'BATCH_TYPE', 'BATCH_NO', 'TRADE_TOTAL']]

        # 加入ID; 模型运行时间
        EMS_FINANCE_LABOR_COST['ID'] = range(len(EMS_FINANCE_LABOR_COST))
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        EMS_FINANCE_LABOR_COST['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")

        # 明确企业所属租户
        EMS_FINANCE_LABOR_COST['CUSTOMER_CODE'] = 'FTA_LG'

        # 重新排序ORG
        FINANCE_LABOR_COST_DETAIL = EMS_FINANCE_LABOR_COST[['ID', 'ORG_CODE', 'BATCH_TYPE', 'BATCH_NO', 'TRADE_TOTAL', 'CHECK_TIME','CUSTOMER_CODE']]

        Write_Oracle().write_oracle('BD_RISK_DETAIL_FINANCE_FN2', FINANCE_LABOR_COST_DETAIL, org_code=self.org_code,
                                    alarm=None)

    def run_clean_fn2(self):
        try:
            self.clean_fn2()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '8b1936b7806342b3bc9c8c3701443329'
    org_code, param_json, base_time = read_log_table(child_task_id)
    FinanceCleanFn2(org_code, child_task_id, params=param_json).run_clean_fn2()