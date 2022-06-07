import sys, os
from os import path

sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_EMS_WORK_INPUT, _name_EMS_WORK_OUTPUT


class ProductionCleanPd1(object):
    def __init__(self, child_task_id, org_code, base_time):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time

    def clean_pd1(self):
        # 处理base_time，获取读数起始时间
        dd = datetime.datetime.strptime(self.base_time, "%Y-%m-%d %H:%M:%S")
        last_day_of_prev_month = dd.replace(day=1) - datetime.timedelta(days=1)
        start_day_of_prev_month = dd.replace(day=1) - datetime.timedelta(days=last_day_of_prev_month.day)

        # 读取加工工单产出表和加工工单耗用表（耗用表默认筛选和产出表相同工单的数据）
        EMS_WORK_OUTPUT = Read_Oracle().read_oracle(sql="""Select * from {} WHERE org_code = '{}' and CAPXACTION != 'D' 
        and inputdbtime >= to_date('{}','yyyy-mm-dd hh24:mi:ss') """.format(_name_EMS_WORK_OUTPUT, self.org_code,
                                                                            start_day_of_prev_month), database='dbods')

        EMS_WORK_INPUT = Read_Oracle().read_oracle(sql="""Select * from {}  WHERE org_code = '{}' and CAPXACTION != 'D' and wo_no in 
        (Select wo_no from {} WHERE org_code = '{}' and CAPXACTION != 'D' and inputdbtime >= to_date('{}','yyyy-mm-dd hh24:mi:ss') ) """.format(
            _name_EMS_WORK_INPUT, self.org_code, _name_EMS_WORK_OUTPUT, self.org_code, start_day_of_prev_month),
                                                   database='dbods')

        # 筛选字段并清洗数据
        EMS_WORK_OUTPUT = EMS_WORK_OUTPUT[['ORG_CODE', 'BATCH_NO', 'WO_NO', 'COP_G_NO', 'QTY_CO']]
        EMS_WORK_OUTPUT['QTY_CO'] = EMS_WORK_OUTPUT['QTY_CO'].apply(lambda x: ''.join(x.split(',')))
        EMS_WORK_OUTPUT['QTY_CO'] = EMS_WORK_OUTPUT['QTY_CO'].apply(lambda x: float(x))
        EMS_WORK_INPUT = EMS_WORK_INPUT[['ORG_CODE', 'WO_NO', 'COP_G_NO', 'QTY_CO']]
        EMS_WORK_INPUT['QTY_CO'] = EMS_WORK_INPUT['QTY_CO'].apply(lambda x: ''.join(x.split(',')))
        EMS_WORK_INPUT['QTY_CO'] = EMS_WORK_INPUT['QTY_CO'].apply(lambda x: float(x))

        # 计算（料号粒度）耗用量和产出量
        create_sum = EMS_WORK_OUTPUT.groupby(['ORG_CODE', 'BATCH_NO', 'WO_NO', 'COP_G_NO'], as_index=False)[
            'QTY_CO'].sum()
        spend_sum = EMS_WORK_INPUT.groupby(['ORG_CODE', 'WO_NO', 'COP_G_NO'], as_index=False)['QTY_CO'].sum()
        MX_PRO_GDHY = pd.merge(create_sum, spend_sum, left_on=['ORG_CODE', 'WO_NO'], right_on=['ORG_CODE', 'WO_NO'],
                               how='left')

        # 过滤掉空行
        MX_PRO_GDHY = MX_PRO_GDHY.dropna(axis=0, how='any')

        # 过滤投入量为0的行
        MX_PRO_GDHY = MX_PRO_GDHY[~MX_PRO_GDHY['QTY_CO_y'].isin([0.0])]
        # 计算产出/投入的耗用比
        MX_PRO_GDHY['OUT_IN_RATIO'] = MX_PRO_GDHY['QTY_CO_x'].map(float) / MX_PRO_GDHY['QTY_CO_y'].map(float)

        # 字段重命名
        PRO_GDHY_DETAIL = MX_PRO_GDHY.rename(
            columns={'COP_G_NO_x': 'COP_G_NO_OUT', 'COP_G_NO_y': 'COP_G_NO_IN', 'QTY_CO_x': 'QTY_CO_OUT',
                     'QTY_CO_y': 'QTY_CO_IN'})

        # 更新ID; 模型运行时间
        PRO_GDHY_DETAIL['ID'] = range(len(PRO_GDHY_DETAIL))
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        PRO_GDHY_DETAIL['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        
        # 明确企业所属租户
        PRO_GDHY_DETAIL['CUSTOMER_CODE'] = 'FTA_LG'

        # 整理结果表
        PRO_GDHY_DETAIL = PRO_GDHY_DETAIL[
            ['ID', 'ORG_CODE', 'BATCH_NO', 'WO_NO', 'COP_G_NO_OUT', 'QTY_CO_OUT', 'COP_G_NO_IN', 'QTY_CO_IN',
             'OUT_IN_RATIO', 'CHECK_TIME', 'CUSTOMER_CODE']]

        # 考虑到读写函数稳定性，将数值列转为字符串类型
        PRO_GDHY_DETAIL['QTY_CO_OUT'] = PRO_GDHY_DETAIL['QTY_CO_OUT'].map(str)
        PRO_GDHY_DETAIL['QTY_CO_IN'] = PRO_GDHY_DETAIL['QTY_CO_IN'].map(str)
        PRO_GDHY_DETAIL['OUT_IN_RATIO'] = PRO_GDHY_DETAIL['OUT_IN_RATIO'].map(str)

        # 读入数据库
        Write_Oracle().write_oracle('BD_RISK_DETAIL_PRODUCTION_PD1', PRO_GDHY_DETAIL, org_code=self.org_code,
                                    alarm=None)

    def run_clean_pd1(self):
        try:
            self.clean_pd1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '0001_0009'
    org_code, param_json, base_time = read_log_table(child_task_id)
    ProductionCleanPd1(child_task_id, org_code, base_time).run_clean_pd1()
