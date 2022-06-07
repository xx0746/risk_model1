import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"F:/bdrisk_model/risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *

class ScorePb(object):
    def __init__(self, child_task_id, org_code, params):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code

    def score_public(self):
        model_code = 'PUBLIC'

        #读取企业名称
        p0=f'''select ORG_CODE,TRADE_CODE,LABEL_CODE,ORG_NAME from {TableList.BD_RISK_CORP_INFO_BASIC.value} WHERE ORG_CODE LIKE '{self.org_code}'  '''
        corp_list = Read_Oracle().read_oracle(sql= p0, database = 'dbalarm')
        if len(corp_list) == 0:
            hg_id = ''
            org_type = ''
            org_name = None
        else:
            hg_id = corp_list.loc[0,'TRADE_CODE']
            org_name = corp_list.loc[0,'ORG_NAME']
            org_type = corp_list.loc[0,'LABEL_CODE']
            org_type = org_type.split(sep=',')[0]

        if org_name is None:
            print(f'Error: Corp Not Found. org_code = {self.org_code}')
            return

        # 读取企业库存模块的结果表数据
        sql=f"""\
        select RISK_LABEL from MX_PUBLIC_ELECTRIC WHERE ISCURRENT = 1 AND CORP_NAME LIKE '{org_name}' \
        """
        pb1_result=Read_Oracle().read_oracle(sql=sql,database='dbalarm')

        sql=f"""
        select RISK_LABEL from MX_PUBLIC_INSURANCE WHERE ISCURRENT = 1 AND CORP_NAME LIKE '{org_name}' \
        """
        pb2_result=Read_Oracle().read_oracle(sql=sql,database='dbalarm')

        # if ((pb1_result is None) and (pb2_result is None)):
        #     print(f'Error: No public data of this corp found. org_code = {self.org_code}, org_name = {org_name}')
        #     return

        if len(pb1_result) != 0:
            sc1 = pb1_result.copy()
            sc1['SCORE'] = pb1_result['RISK_LABEL'].apply(lambda x : 0.1 if x=='比值正常' else -0.1)
            sc1_total = sc1['SCORE'].sum()
            sc1_event = sc1_total*10
        else:
            print("Warnning: public_electric data not found.")
            sc1_total = 0
            sc1_event = 0

        if len(pb2_result) != 0:
            sc2 = pb2_result.copy()
            sc2['SCORE'] = pb2_result['RISK_LABEL'].apply(lambda x : 0.1 if x=='比值正常' else -0.1)
            sc2_total = sc2['SCORE'].sum()
            sc2_event = sc2_total*10
        else:
            print("Warnning: public_insurance data not found.")
            sc2_total = 0
            sc2_event = 0

        business_time = (datetime.date.today().replace(day=1) - datetime.timedelta(1)).replace(day=1)

        sc_total = sc1_total + sc2_total
        if sc_total>=0:
            sc_total = 100
        else:
            sc_total = 100 + sc_total

        df_result = pd.DataFrame()
        if sc_total < 100:
            data = {'SCORE':[sc1_total,sc2_total],'LABEL':['水电数据异常','社保数据异常'],'EVENT_NUMBER':[sc1_event,sc2_event]}
            df_result = pd.DataFrame(data,index=range(2))
            df_result = df_result[df_result['SCORE']<0].copy()
            df_result['MODEL_CODE'] = model_code
            df_result['HG_ORG_ID'] = hg_id
            df_result['CHILD_MODEL_CODE'] = model_code
            df_result['BUSINESS_TIME'] = business_time
            df_result['BUSINESS_TIME'] = df_result['BUSINESS_TIME'].astype('datetime64')
            df_result = df_result.reset_index(drop=True)
            df_result = df_result.reset_index().rename(columns={'index': 'ID'})
            df_result['ORG_CODE'] = self.org_code
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_DTL', df_result, org_code = self.org_code,alarm = [model_code,''])

        data = {'SCORE':sc_total,'CHILD_MODEL_CODE':'PUBLIC','ID':0}
        df_cm = pd.DataFrame(data,index=range(1))
        df_cm['ORG_CODE'] = self.org_code
        df_cm['MODEL_CODE'] = model_code
        df_cm['HG_ORG_ID'] = hg_id
        df_cm['BUSINESS_TIME'] = business_time
        df_cm['BUSINESS_TIME'] = df_cm['BUSINESS_TIME'].astype('datetime64')
        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_CM', df_cm, org_code = self.org_code, alarm = [model_code,''])

        data2 = {'SCORE':sc_total,'ID':0}
        df_score = pd.DataFrame(data2,index=range(1))
        df_score['ORG_CODE'] = self.org_code
        df_score['HG_ORG_ID'] = hg_id
        df_score['ORG_TYPE'] = org_type
        df_score['MODEL_CODE'] = model_code
        df_score['BUSINESS_TIME'] = business_time
        df_score['BUSINESS_TIME'] = df_score['BUSINESS_TIME'].astype('datetime64')
        
        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE', df_score, org_code = self.org_code, alarm = [model_code,''])

        

    def run_score_public(self):
        try:
            self.score_public()
            exec_status = 1
        except Exception as e:
            errorStr = f'''{e}'''
            errorStr = errorStr.replace("'", " ")
            errorStr = errorStr[-3900:]
            logger.exception(errorStr)
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()

if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = 'fef0424300744209860f05cd2fa3daaa'
    org_code, params, base_time = read_log_table(child_task_id)
    # org_code = '913100006929421297'
    ScorePb(child_task_id, org_code, "").run_score_public()