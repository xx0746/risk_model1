import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *

class ScoreCt(object):
    def __init__(self, child_task_id, org_code, params):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code

    def score_custom(self):
        #读取企业名称
        p0=f'''select ORG_CODE,ORG_NAME from {TableList.BD_RISK_CORP_INFO_BASIC.value} WHERE ORG_CODE LIKE '{self.org_code}'  '''
        corp_list = Read_Oracle().read_oracle(sql= p0, database = 'dbdm')
        if corp_list is not None:
            org_name = corp_list.loc[0,'ORG_NAME']
        else:
            print(f'Error: Corp Not Found. org_code = {self.org_code}')
            return

        sc_total = 100

        CUSTOM_RESULT = pd.DataFrame(data=[[0, self.org_code, 'CUSTOM', sc_total]], columns=['ID', 'ORG_CODE', 'MODEL_CODE', 'SCORE'])
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        CUSTOM_RESULT['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        CUSTOM_RESULT['CUSTOMER_CODE'] = 'FTA_LG'
        Write_Oracle().write_oracle('BD_RISK_CORP_SCORE_DISPLAY',CUSTOM_RESULT, org_code = self.org_code, alarm = ['CUSTOM',''])

    def run_score_custom(self):
        try:
            self.score_custom()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id = self.child_task_id, exec_status=exec_status).write_log()

if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    # child_task_id = sys.argv[1]
    child_task_id = 'fef0424300744209860f05cd2fa3daaa'
    org_code, params, base_time = read_log_table(child_task_id)
    # org_code = '91310000132612172J'
    ScoreCt(child_task_id, org_code, params).run_score_custom()