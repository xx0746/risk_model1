import sys, os
from os import path

import pandas as pd

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\WorkSpace\Pycharm_Space\bdrisk_model\risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class TongGuanModel(object):
    def __init__(self, org_code, child_task_id, params):
        self.org_code = org_code
        self.child_task_id = child_task_id
        self.now = datetime.datetime.now()
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

    def model_TongGuan(self):
        sql = """ SELECT * FROM {} WHERE iscurrent =1 AND (C_SCORE < 50  OR TYPE_FIRST < 50)""".format('DW_CUS_RC.BD_RISK_CROSS_TRADE_DETAIL_PASS_CLEAN')

        TongGuan_data = Read_Oracle().read_oracle(sql=sql, database='dbdw')
        if TongGuan_data.empty:
            Result_TG = pd.DataFrame()
        else:
            TongGuan_data['ENTRY_LABEL_NAME'] = TongGuan_data[['MANUAL_ENTRY_COUNT','C_SCORE']].apply(lambda x: """该司近半年人工审单处置次数为{}次""".format(str(int(x['MANUAL_ENTRY_COUNT'])))
                                                                                                                            if x['C_SCORE'] < 50 else '正常',axis = 1)

            TongGuan_data['SEIZED_LABEL_NAME'] = TongGuan_data['TYPE_FIRST'].apply(lambda x: '该司近半年查获率高于关区平均查获率' if float(x) < 50 else '正常')
            TongGuan_data['TOTAL_SCORE'] = TongGuan_data['C_SCORE'] + TongGuan_data['TYPE_FIRST']
            order_list = ['CUSTOM_ID','CORP_CREDIT_CODE','TRADE_NAME','TRADE_CODE','ENTRY_COUNT', 'MANUAL_ENTRY_COUNT','C_SCORE','TYPE_FIRST','TOTAL_SCORE','ENTRY_LABEL_NAME','SEIZED_LABEL_NAME']
            Result_TG = TongGuan_data[order_list]
            Result_TG = (Result_TG.reset_index()).rename(columns={'index': 'ID','CUSTOM_ID':'CUSTOMS_CODE'})
            Result_TG['BUSINESS_END_TIME'] = self.now  ###模型运行时间月份第一天
            Result_TG['MODEL_TIME'] = self.now
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_RESULT_PASS_SUMMARY', Result_TG, None, alarm=None)

    def Write_To_Score(self):
        sql = """ SELECT * FROM {} WHERE iscurrent =1 """.format('DW_CUS_RC.BD_RISK_CROSS_TRADE_DETAIL_PASS_CLEAN')

        Score_Data = Read_Oracle().read_oracle(sql=sql, database='dbdw')
        Score_Data['TOTAL_SCORE'] = Score_Data['C_SCORE'] + Score_Data['TYPE_FIRST']
        bunesstime = (datetime.date.today().replace(day=1) - datetime.timedelta(1)).replace(day=1)
        bunesstime = bunesstime.strftime("%Y-%m-%d %H:%M:%S")
        bunesstime = datetime.datetime.strptime(bunesstime, '%Y-%m-%d %H:%M:%S')
        ###Write to score
        Score_Df = Score_Data.copy(deep = True)
        Score_Df['ORG_TYPE'] = Score_Df['CORP_CREDIT_CODE']
        Score_Df['MODEL_CODE'] = 'THROUGH_CUSTOM'
        Score_Df['BUSINESS_END_TIME'] = bunesstime
        need_list = ['CORP_CREDIT_CODE','TRADE_CODE','ORG_TYPE', 'BUSINESS_END_TIME','MODEL_CODE','TOTAL_SCORE']
        Score_Df = Score_Df[need_list]
        Score_Df = (Score_Df.reset_index()).rename(columns={'index': 'ID',
                                                            'CORP_CREDIT_CODE': 'ORG_CODE',
                                                            'TRADE_CODE': 'HG_ORG_ID',
                                                            'BUSINESS_END_TIME': 'BUSINESS_TIME',
                                                            'TOTAL_SCORE': 'SCORE'
                                                            })
        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE', Score_Df, None, alarm=None,
                                          alarm_code='THROUGH_CUSTOM')

        ###Write to score_cm
        Score_cm = Score_Df.copy(deep = True)
        Score_cm['CHILD_MODEL_CODE'] = Score_cm['MODEL_CODE']
        cm_list = ['ID','ORG_CODE','HG_ORG_ID','BUSINESS_TIME','MODEL_CODE','CHILD_MODEL_CODE','SCORE']
        Score_cm = Score_cm[cm_list]
        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_CM', Score_cm, None, alarm=None,
                                          alarm_code='THROUGH_CUSTOM')

        ###write to
        DEL_DATA = Score_Data.copy(deep = True)
        DEL_DATA = DEL_DATA.loc[~((DEL_DATA['C_SCORE'] == 50) & (DEL_DATA['TYPE_FIRST'] == 50))]

        if DEL_DATA.empty:
            DEL_DATA =  pd.DataFrame()
        else:

            def fun_label(x):
                return '人工审单扣'+str(50-(x['C_SCORE']))+"分,"+"查获率扣"+str(50-(x['TYPE_FIRST']))+"分"

            DEL_DATA['LABEL'] = DEL_DATA[['C_SCORE','TYPE_FIRST']].apply(lambda x: fun_label(x),axis = 1)
            DEL_DATA['EVENT_NUMBER'] = 1
            DEL_DATA['SCORE'] = DEL_DATA['TOTAL_SCORE'] - 100
            DEL_DATA['BUSINESS_TIME'] = bunesstime
            DEL_DATA['MODEL_CODE'] = 'THROUGH_CUSTOM'
            DEL_DATA['CHILD_MODEL_CODE'] = 'THROUGH_CUSTOM'
            DEL_DATA = DEL_DATA.rename(columns={ 'CORP_CREDIT_CODE': 'ORG_CODE',
                                                 'TRADE_CODE': 'HG_ORG_ID'
                                                                })
            DEL_List = ['ORG_CODE','HG_ORG_ID','BUSINESS_TIME','MODEL_CODE','CHILD_MODEL_CODE','LABEL','EVENT_NUMBER','SCORE']
            DEL_DATA = DEL_DATA[DEL_List]
            DEL_DATA = (DEL_DATA.reset_index()).rename(columns={'index': 'ID'})
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE_DTL', DEL_DATA, None, alarm=None,
                                              alarm_code='THROUGH_CUSTOM')

    def Run_Model_TongGuan(self):
        try:
            self.model_TongGuan()
            self.Write_To_Score()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            print(1)
            #Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()

if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    if params_global.is_test:
        child_task_id = 'd03c492250e344b79d1ef9453d47f4ea'
    else:
        child_task_id = sys.argv[1]
    #org_code, param_json, base_time = read_log_table(child_task_id)
    org_code = None
    TongGuanModel(child_task_id, org_code, 30).Run_Model_TongGuan()
