import sys, os
from os import path

sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_OPENING_INVENTORY, _name_OPENING_INVENTORY_DETAIL, _name_WAREHOUSE_STOCK_BILL


class StockCleanSt2(object):
    def __init__(self, org_code, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time

    def clean_st2(self):
        # 读取期初库存head表（iscurrent = 1）
        STOCK_OPENING_HEAD = Read_Oracle().read_oracle(sql=""" select * from {} where iscurrent = 1 and 
                                                                 CREDIT_CODE = '{}' """.format(_name_OPENING_INVENTORY,
                                                                                               self.org_code),
                                                       database='dbods')
        # 清洗期末时间
        end_time = datetime.datetime.strptime(self.base_time[0:10], "%Y-%m-%d")
        # 根据期末时间，筛选期初库存表（取离基准时间最近的一条记录，且该期初时间小于基准时间）
        STOCK_OPENING_HEAD = STOCK_OPENING_HEAD[STOCK_OPENING_HEAD['OPT_DATE'] <= end_time]
        STOCK_OPENING_HEAD = STOCK_OPENING_HEAD[STOCK_OPENING_HEAD.OPT_DATE == STOCK_OPENING_HEAD.OPT_DATE.max()]

        # 获取期初库存明细表的PID和期初时间
        STOCK_OPENING_DETAIL_PID = int(STOCK_OPENING_HEAD['ID'])
        start_time = np.unique(STOCK_OPENING_HEAD.OPT_DATE)[0]
        # 根据PID，筛选期初库存明细表
        STOCK_OPENING_DETAIL = Read_Oracle().read_oracle(sql=""" select * from {} where iscurrent = 1 
                                                                and PID = {} """.format(_name_OPENING_INVENTORY_DETAIL,
                                                                                        STOCK_OPENING_DETAIL_PID),
                                                         database='dbods')
        # 转换“数量”为数字
        STOCK_OPENING_DETAIL['DCL_QTY'] = STOCK_OPENING_DETAIL['DCL_QTY'].map(float)
        # 汇总期初库存表
        STOCK_OPENING_DETAIL = STOCK_OPENING_DETAIL.groupby(['GDS_MTNO'])['DCL_QTY'].sum().reset_index()
        STOCK_OPENING_DETAIL = STOCK_OPENING_DETAIL.rename(columns={'GDS_MTNO': 'COP_G_NO'})

        # 读取出入库表（限制出入库类型）,并根据输入企业信用代码筛选出入库表
        STOCK_BILL = Read_Oracle().read_oracle(sql=""" select STOCK_BILL_TYPE, ACTRUAL_STOCK_DATE, COP_G_NO, QTY from {} where CAPXACTION != 'D' and business_type in ('2','3','4') 
                                                             and ORG_CODE = '{}' and QTY is not null """.format(
            _name_WAREHOUSE_STOCK_BILL, self.org_code), database='dbods')
        # 根据期初和期末时间，筛选出入库表
        STOCK_BILL = STOCK_BILL[
            (STOCK_BILL['ACTRUAL_STOCK_DATE'] >= start_time) & (STOCK_BILL['ACTRUAL_STOCK_DATE'] <= end_time)]
        # 转换“数量”为数字
        STOCK_BILL['QTY'] = STOCK_BILL['QTY'].map(float)
        # 转换“出入库类型”字段数据类型
        STOCK_BILL['STOCK_BILL_TYPE'] = STOCK_BILL['STOCK_BILL_TYPE'].map(int)
        # 对"出入库类型"进行转化便于计算
        STOCK_BILL['STOCK_BILL_TYPE'] = STOCK_BILL['STOCK_BILL_TYPE'].map(lambda x: -1 if x == 2 else 1)
        # 将出库的数量变为负数
        STOCK_BILL['QTY'] = STOCK_BILL['QTY'] * STOCK_BILL['STOCK_BILL_TYPE']
        # 汇总出入库表
        STOCK_BILL = STOCK_BILL.groupby(['COP_G_NO'])['QTY'].sum().reset_index()

        # 汇总期初和出入库表，生成期末库存表
        STOCK_END_DETAIL = pd.merge(STOCK_BILL, STOCK_OPENING_DETAIL, how='left', on=['COP_G_NO'])
        STOCK_END_DETAIL = STOCK_END_DETAIL.rename(columns={'QTY': 'QTY_CHANGE', 'DCL_QTY': 'QTY_BEFORE'})
        # 将期初库存为空的行值设为0
        STOCK_END_DETAIL = STOCK_END_DETAIL.fillna(0)
        # 计算期末库存
        STOCK_END_DETAIL['QTY_AFTER'] = STOCK_END_DETAIL['QTY_CHANGE'] + STOCK_END_DETAIL['QTY_BEFORE']

        # 加入企业信息; 期初期末时间；ID; 模型运行时间
        STOCK_END_DETAIL['ORG_CODE'] = self.org_code
        STOCK_END_DETAIL['STARTDT'] = start_time
        STOCK_END_DETAIL['ENDDT'] = end_time
        STOCK_END_DETAIL['ID'] = range(len(STOCK_END_DETAIL))
        detail_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STOCK_END_DETAIL['CHECK_TIME'] = datetime.datetime.strptime(detail_now, "%Y-%m-%d %H:%M:%S")

        # 明确企业所属租户
        STOCK_END_DETAIL['CUSTOMER_CODE'] = 'FTA_LG'

        # 重新排序
        STOCK_END_DETAIL = STOCK_END_DETAIL[
            ['ID', 'ORG_CODE', 'STARTDT', 'ENDDT', 'COP_G_NO', 'QTY_BEFORE', 'QTY_CHANGE', 'QTY_AFTER', 'CHECK_TIME', 'CUSTOMER_CODE']]

        # 写入数据库
        Write_Oracle().write_oracle('BD_RISK_DETAIL_STOCK_ST2', STOCK_END_DETAIL, org_code=self.org_code, alarm=None)

    def run_clean_st2(self):
        try:
            self.clean_st2()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '7e9f52075df94820a8132d2343990b75'
    org_code, param_json, base_time = read_log_table(child_task_id)
    StockCleanSt2(org_code, base_time, child_task_id).run_clean_st2()
