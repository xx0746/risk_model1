import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_DETAIL_WAREHOUSE_WH2


class WarehouseModelWh2(object):
    def __init__(self, child_task_id, org_code, params):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.ratio_high = json.loads(params)['ratio_high']
        self.ratio_low = json.loads(params)['ratio_low']

    def model_wh2(self):
        # 读取明细表
        WAREHOUSE_ORDER_RESULT = Read_Oracle().read_oracle(sql= """ select * from {} where org_code = '{}' 
                                                                and iscurrent = 1 and CUSTOMER_CODE = 'FTA_LG' """.format(_name_BD_RISK_DETAIL_WAREHOUSE_WH2, self.org_code), database = 'dbdm')
        
        # 阈值计算函数
        def Qfunc(df):
            Q1 = np.percentile(df['DIFF_DAYS'], 25)
            Q3 = np.percentile(df['DIFF_DAYS'], 75)
            IQR = Q3 - Q1
            outlier_step = 1.5 * IQR
            return (Q3 + outlier_step), (Q1 - outlier_step)
        
        # 打标签函数
        def Risk_label(ratio, high, low):
            if ratio > high:
                return '订单交期延误'
            if ratio < low:
                return '订单过早交付'
            else:
                return '订单按时完成'
        
        # 计算上下阈值
        cutoff = WAREHOUSE_ORDER_RESULT.groupby(['ORG_CODE', 'CUSTOMER_NAME', 'ORDER_TYPE']).apply(Qfunc)
        cutoff = pd.DataFrame(cutoff).reset_index()
        cutoff['CUTOFF_HIGH'] = cutoff[0].map(lambda x: x[0])
        cutoff['CUTOFF_LOW'] = cutoff[0].map(lambda x: x[1])
        cutoff.drop(0, axis=1, inplace=True)
        
        # 打标签
        WAREHOUSE_ORDER_RESULT = pd.merge(WAREHOUSE_ORDER_RESULT, cutoff, on=['ORG_CODE', 'CUSTOMER_NAME', 'ORDER_TYPE'], how='left')
        WAREHOUSE_ORDER_RESULT['RISK_LABEL'] = WAREHOUSE_ORDER_RESULT.apply(lambda x: Risk_label(x['DIFF_DAYS'], x['CUTOFF_HIGH'], x['CUTOFF_LOW']), axis=1)
        
        # 设置惩罚分数
        ratio = {'过高':float(eval(self.ratio_high)), '过低':float(eval(self.ratio_low))}
        # 计算分数
        WAREHOUSE_ORDER_RESULT['SCORE'] = WAREHOUSE_ORDER_RESULT['RISK_LABEL'].map(lambda x: ratio['过高'] if x == '订单交期延误' else (ratio['过低'] if x == '订单过早交付' else 0))
        
        # 更新ID; 模型运行时间
        WAREHOUSE_ORDER_RESULT['ID'] = range(len(WAREHOUSE_ORDER_RESULT))
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        WAREHOUSE_ORDER_RESULT['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        
        # 明确企业所属租户
        WAREHOUSE_ORDER_RESULT['CUSTOMER_CODE'] = 'FTA_LG'
        
        # 整理结果表
        WAREHOUSE_ORDER_RESULT = WAREHOUSE_ORDER_RESULT[['ID','ORG_CODE','CUSTOMER_NAME','ORDER_TYPE','ORDER_NO','RISK_LABEL','SCORE','CHECK_TIME','CUSTOMER_CODE']]
        
        # 读入数据库
        Write_Oracle().write_oracle('BD_RISK_RESULT_WAREHOUSE_WH2',WAREHOUSE_ORDER_RESULT, org_code=self.org_code, alarm = None)
        
        # 整理预警明细数据，并写入数据库
        RISK_ALARM = WAREHOUSE_ORDER_RESULT[WAREHOUSE_ORDER_RESULT['RISK_LABEL'] != '订单按时完成'].groupby(['RISK_LABEL'], as_index=False)['ID'].count()
        RISK_ALARM = RISK_ALARM.rename(columns={'ID':'ALARM_NUMBER'})
        RISK_ALARM['ALARM_REASON'] = '发现' + RISK_ALARM['ALARM_NUMBER'].astype('str') + '起' + RISK_ALARM['RISK_LABEL'] + '事件'
        RISK_ALARM['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        RISK_ALARM['ORG_CODE'] = self.org_code
        RISK_ALARM['MODEL_CODE'] = 'WAREHOUSE'
        RISK_ALARM['CHILD_MODEL_CODE'] = 'WH2'
        RISK_ALARM['ID'] = range(len(RISK_ALARM))
        # 明确企业所属租户
        RISK_ALARM['CUSTOMER_CODE'] = 'FTA_LG'
        RISK_ALARM = RISK_ALARM[['ID','ORG_CODE','MODEL_CODE','CHILD_MODEL_CODE','ALARM_REASON','ALARM_NUMBER','CHECK_TIME','CUSTOMER_CODE']]
        
        if RISK_ALARM.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle('BD_RISK_ALARM_ITEM',RISK_ALARM, org_code = self.org_code, alarm = ['WAREHOUSE','WH2'])
    
    
    def run_model_wh2(self):
        try:
            self.model_wh2()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id = self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '0002_0020'
    org_code, param_json, base_time = read_log_table(child_task_id)
    WarehouseModelWh2(child_task_id, org_code, params=param_json).run_model_wh2()