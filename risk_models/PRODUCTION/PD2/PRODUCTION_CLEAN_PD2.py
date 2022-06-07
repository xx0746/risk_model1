import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_EMS_TIMECOST_INFO


class ProductionCleanPd2(object):
    def __init__(self, child_task_id, org_code, base_time):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time
        
    def clean_pd2(self):
        # 处理base_time，获取读数起始时间
        dd = datetime.datetime.strptime(self.base_time, "%Y-%m-%d %H:%M:%S")
        last_day_of_prev_month = dd.replace(day=1) - datetime.timedelta(days=1)
        start_day_of_prev_month = dd.replace(day=1) - datetime.timedelta(days=last_day_of_prev_month.day)
        
        # 读取加工工时耗用表并过滤任何数据为空的行
        EMS_TIMECOST_INFO = Read_Oracle().read_oracle(sql = """ Select org_code, batch_no, wo_no, wo_type, TOTAL_ACT_TIMECOST from {} where CAPXACTION != 'D' 
        and org_code = '{}' and batch_no is not null and wo_no is not null and wo_type is not null and TOTAL_ACT_TIMECOST is not null 
        and inputdbtime >= to_date('{}','yyyy-mm-dd hh24:mi:ss') """.format(_name_EMS_TIMECOST_INFO ,self.org_code, start_day_of_prev_month), database = 'dbods')
        
        # 清洗“合计实动工时”字段
        EMS_TIMECOST_INFO['TOTAL_ACT_TIMECOST'] = EMS_TIMECOST_INFO['TOTAL_ACT_TIMECOST'].apply(lambda x: ''.join(x.split(',')))
        EMS_TIMECOST_INFO['TOTAL_ACT_TIMECOST'] = EMS_TIMECOST_INFO['TOTAL_ACT_TIMECOST'].apply(lambda x: x.replace('-',''))
        EMS_TIMECOST_INFO['TOTAL_ACT_TIMECOST'] = EMS_TIMECOST_INFO['TOTAL_ACT_TIMECOST'].map(float)
        
        # 根据企业-批次号-工单类型-工单号粒度，聚合生产工时
        PRO_SCGS_DETAIL = EMS_TIMECOST_INFO.groupby(['ORG_CODE', 'BATCH_NO', 'WO_TYPE', 'WO_NO'], as_index=False)['TOTAL_ACT_TIMECOST'].sum()
        # 字段重命名
        PRO_SCGS_DETAIL = PRO_SCGS_DETAIL.rename(columns={'TOTAL_ACT_TIMECOST':'TIMECOST'})
        
        # 更新ID; 模型运行时间
        PRO_SCGS_DETAIL['ID'] = range(len(PRO_SCGS_DETAIL))
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        PRO_SCGS_DETAIL['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        
        # 明确企业所属租户
        PRO_SCGS_DETAIL['CUSTOMER_CODE'] = 'FTA_LG'
    
        # 整理结果表
        PRO_SCGS_DETAIL = PRO_SCGS_DETAIL[['ID', 'ORG_CODE', 'BATCH_NO', 'WO_TYPE', 'WO_NO', 'TIMECOST', 'CHECK_TIME','CUSTOMER_CODE']]
        
        # 读入写入数据库
        Write_Oracle().write_oracle('BD_RISK_DETAIL_PRODUCTION_PD2',PRO_SCGS_DETAIL, org_code=self.org_code, alarm=None)

    def run_clean_pd2(self):
        try:
            self.clean_pd2()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id = self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    #child_task_id = '0001_0010'
    org_code, param_json, base_time = read_log_table(child_task_id)
    ProductionCleanPd2(child_task_id, org_code, base_time).run_clean_pd2()