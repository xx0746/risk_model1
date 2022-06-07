import sys, os
from os import path

sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_DETAIL_FINANCE_FN2


class FinanceModelFn2(object):
    def __init__(self, child_task_id, org_code, params):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.ratio_high = json.loads(params)['ratio_high']
        self.ratio_low = json.loads(params)['ratio_low']

    def model_fn2(self):
        # 读入人工成本明细表
        FINANCE_LABOR_COST_DETAIL = Read_Oracle().read_oracle(
            sql=""" select * from {} where iscurrent = 1 and org_code = '{}' and CUSTOMER_CODE = 'FTA_LG' """.format(
                _name_BD_RISK_DETAIL_FINANCE_FN2, self.org_code), database='dbdm')

        # 根据历史计算阈值函数
        def func(df):
            Q1 = np.percentile(df['TRADE_TOTAL'], 25)
            Q3 = np.percentile(df['TRADE_TOTAL'], 75)
            IQR = Q3 - Q1
            outlier_step = 1.5 * IQR
            return (Q1 - outlier_step), (Q3 + outlier_step)

        # 根据历史计算阈值
        df_cutoff = FINANCE_LABOR_COST_DETAIL
        df_cutoff = df_cutoff.groupby(['ORG_CODE', 'BATCH_TYPE']).apply(func).reset_index()
        df_cutoff.rename(columns={0: 'cutoff'}, inplace=True)
        df_cutoff['cutoff_low'] = df_cutoff['cutoff'].map(lambda x: x[0])
        df_cutoff['cutoff_high'] = df_cutoff['cutoff'].map(lambda x: x[1])

        # 打标签函数
        def risk_tag(value, cutoff_low, cutoff_high):
            if value < cutoff_low:
                return '人工成本过低'
            elif value > cutoff_high:
                return '人工成本超支'
            else:
                return '人工成本正常'

        # 打标签
        FINANCE_LABOR_COST_RESULT = FINANCE_LABOR_COST_DETAIL.merge(df_cutoff, on=['ORG_CODE', 'BATCH_TYPE'],
                                                                    how='left')
        FINANCE_LABOR_COST_RESULT['RISK_LABEL'] = FINANCE_LABOR_COST_RESULT.apply(
            lambda x: risk_tag(x['TRADE_TOTAL'], x['cutoff_low'], x['cutoff_high']), axis=1)

        # 算分
        ratio = {'过高': float(eval(self.ratio_high)), '过低': float(eval(self.ratio_low))}
        # 计算分数
        FINANCE_LABOR_COST_RESULT['SCORE'] = FINANCE_LABOR_COST_RESULT['RISK_LABEL'].map(
            lambda x: ratio['过高'] if x == '人工成本超支' else (ratio['过低'] if x == '人工成本过低' else 0))

        # 删去不用的列
        FINANCE_LABOR_COST_RESULT.drop(
            columns=['ID', 'CHECK_TIME', 'TRADE_TOTAL', 'cutoff', 'cutoff_low', 'cutoff_high'], inplace=True)

        # 更新ID; 模型运行时间
        FINANCE_LABOR_COST_RESULT['ID'] = range(len(FINANCE_LABOR_COST_RESULT))
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        FINANCE_LABOR_COST_RESULT['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")

        # 明确企业所属租户
        FINANCE_LABOR_COST_RESULT['CUSTOMER_CODE'] = 'FTA_LG'

        # 整理结果表并写入
        FINANCE_LABOR_COST_RESULT = FINANCE_LABOR_COST_RESULT[
            ['ID', 'ORG_CODE', 'BATCH_TYPE', 'BATCH_NO', 'RISK_LABEL', 'SCORE', 'CHECK_TIME', 'CUSTOMER_CODE']]
        Write_Oracle().write_oracle('BD_RISK_RESULT_FINANCE_FN2', FINANCE_LABOR_COST_RESULT, org_code=self.org_code,
                                    alarm=None)

        # 整理预警明细数据，并写入数据库
        RISK_ALARM = \
        FINANCE_LABOR_COST_RESULT[FINANCE_LABOR_COST_RESULT['RISK_LABEL'] != '人工成本正常'].groupby(['RISK_LABEL'],
                                                                                               as_index=False)[
            'ID'].count()
        RISK_ALARM = RISK_ALARM.rename(columns={'ID': 'ALARM_NUMBER'})
        RISK_ALARM['ALARM_REASON'] = '发现' + RISK_ALARM['ALARM_NUMBER'].astype('str') + '起' + RISK_ALARM[
            'RISK_LABEL'] + '事件'
        RISK_ALARM['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        RISK_ALARM['ORG_CODE'] = self.org_code
        RISK_ALARM['MODEL_CODE'] = 'FINANCE'
        RISK_ALARM['CHILD_MODEL_CODE'] = 'FN2'
        RISK_ALARM['ID'] = range(len(RISK_ALARM))
        # 明确企业所属租户
        RISK_ALARM['CUSTOMER_CODE'] = 'FTA_LG'
        RISK_ALARM = RISK_ALARM[
            ['ID', 'ORG_CODE', 'MODEL_CODE', 'CHILD_MODEL_CODE', 'ALARM_REASON', 'ALARM_NUMBER', 'CHECK_TIME',
             'CUSTOMER_CODE']]

        if RISK_ALARM.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle('BD_RISK_ALARM_ITEM', RISK_ALARM, org_code=self.org_code,
                                        alarm=['FINANCE', 'FN2'])

    def run_model_fn2(self):
        try:
            self.model_fn2()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '685ca74d2f384c1ba942cd2f70c85131'
    org_code, param_json, base_time = read_log_table(child_task_id)
    FinanceModelFn2(child_task_id, org_code, params=param_json).run_model_fn2()
