import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk_model\risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_DETAIL_STOCK_ST1


class StockModelSt1(object):
    def __init__(self, child_task_id, org_code, params):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.cutoff_high = json.loads(params).get('cutoff_high', 0)
        self.cutoff_low = json.loads(params).get('cutoff_low', 0)
        self.ratio_high = json.loads(params).get('ratio_high', 0)
        self.ratio_low = json.loads(params).get('ratio_low', 0)

    def model_st1(self):
        # 读取明细表
        STOCK_END_RESULT = Read_Oracle().read_oracle(
            sql=""" select * from {} where iscurrent = 1 and org_code = '{}' and CUSTOMER_CODE = 'FTA_LG' """.format(
                _name_BD_RISK_DETAIL_STOCK_ST1, self.org_code), database='dbdm')
        # 删去不用的列
        STOCK_END_RESULT.drop(columns=['ID', 'CHECK_TIME', 'ISCURRENT', 'LASTUPDATE'], inplace=True)

        # 读取参数
        cutoff = {'高阈值': float(self.cutoff_high), '低阈值': float(self.cutoff_low)}
        ratio = {'盈': float(eval(self.ratio_high)), '亏': float(eval(self.ratio_low))}

        # 通过阈值计算标签
        STOCK_END_RESULT['RISK_LABEL'] = STOCK_END_RESULT['QTY_AFTER'].map(
            lambda x: '库存过高' if x > cutoff['高阈值'] else ('负库存' if x < cutoff['低阈值'] else '库存正常'))
        # 计算分数
        STOCK_END_RESULT['SCORE'] = STOCK_END_RESULT['RISK_LABEL'].map(
            lambda x: ratio['盈'] if x == '库存过高' else (ratio['亏'] if x == '负库存' else 0))

        # 更新ID和运行时间
        STOCK_END_RESULT = STOCK_END_RESULT.reset_index().rename(columns={'index': 'ID'})
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STOCK_END_RESULT['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")

        # 删去不用的列
        STOCK_END_RESULT.drop(columns=['QTY_BEFORE', 'QTY_CHANGE', 'QTY_AFTER'], inplace=True)

        # 明确企业所属租户
        STOCK_END_RESULT['CUSTOMER_CODE'] = 'FTA_LG'

        # 重新排序
        STOCK_END_RESULT = STOCK_END_RESULT[
            ['ID', 'ORG_CODE', 'STARTDT', 'ENDDT', 'WH_NO', 'WH_LOC', 'COP_G_NO', 'RISK_LABEL', 'SCORE', 'CHECK_TIME',
             'CUSTOMER_CODE']]

        # 写入数据库
        Write_Oracle().write_oracle('BD_RISK_RESULT_STOCK_ST1', STOCK_END_RESULT, org_code=self.org_code, alarm=None)

        # 整理预警明细数据，并写入数据库
        RISK_ALARM = STOCK_END_RESULT[STOCK_END_RESULT['RISK_LABEL'] != '库存正常'].groupby(['RISK_LABEL'], as_index=False)[
            'ID'].count()
        RISK_ALARM = RISK_ALARM.rename(columns={'ID': 'ALARM_NUMBER'})
        RISK_ALARM['ALARM_REASON'] = '发现' + RISK_ALARM['ALARM_NUMBER'].astype('str') + '起' + RISK_ALARM[
            'RISK_LABEL'] + '事件'
        RISK_ALARM['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        RISK_ALARM['ORG_CODE'] = self.org_code
        RISK_ALARM['MODEL_CODE'] = 'STOCK'
        RISK_ALARM['CHILD_MODEL_CODE'] = 'ST1'
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
                                        alarm=['STOCK', 'ST1'])

    def run_model_st1(self):
        try:
            self.model_st1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    if params_global.is_test:
        child_task_id = '1b3612f68ff244629af862031c21d8b9'
    else:
        child_task_id = sys.argv[1]
    print(child_task_id)
    org_code, param_json, base_time = read_log_table(child_task_id)
    StockModelSt1(child_task_id, org_code, params=param_json).run_model_st1()
