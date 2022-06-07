import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\WorkSpace\Pycharm_Space\bdrisk_model\risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class ChemicalModel(object):
    def __init__(self, child_task_id, org_code):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)
        # 参数读取
        self.org_code = None
        self.Time_Window = 30

    def model_CHEMICAL(self):
        # READ DETAIL DATA
        sql=""" SELECT * FROM {} WHERE iscurrent = 1 AND ACCESS_TIME_DIFF > {}""".format('DW_CUS_RC.BD_RISK_CROSS_TRADE_DETAIL_CHEMICAL_CLEAN',self.Time_Window)
        chemical_result = Read_Oracle().read_oracle(sql = sql, database='dbalarm')

        if chemical_result.empty:
            print('There No exception')
        else:
            now_date = datetime.datetime.now()
            chemical_result['MODEL_TIME'] = now_date
            chemical_result['LABEL'] = '危化品申报后长期未出库'
            chemical_result['ACCESS_TIME_DIFF'] = chemical_result['ACCESS_TIME_DIFF'].map(int)
            chemical_result['BUSINESS_TIME'] = now_date
            chemical_result['SCORE'] = 0
            chemical_result_cp = chemical_result.copy(deep=True)
            # adjust field order
            order_list =['ID','BIZOP_ETPS_SCCD','BIZOP_ETPS_NM','BUSINESS_TIME','MODEL_TIME','BUSINESS_TYPECD','INVT_IOCHKPT_STUCD','ACCESS_TIME','RLT_ENTRY_NO','WH_REC_PREENT_NO', 'ACCESS_TIME_DIFF','LABEL','SCORE']
            chemical_result = chemical_result[order_list]
            new_col = ['ID','ORG_CODE','ORG_NAME','BUSINESS_TIME','MODEL_TIME','BUSINESS_TYPE','STATUS','ACCESS_TIME','RLT_ENTRY_NO','WH_REC_PREENT_NO','DURATION','LABEL','SCORE']
            chemical_result.columns = new_col
            chemical_result['BUSINESS_TYPE'] = chemical_result['BUSINESS_TYPE'].map(str)
            chemical_result['STATUS'] = chemical_result['STATUS'].map(str)
            # write to batabase
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_RESULT_CHEMICAL_WARAING', chemical_result, org_code=None,alarm=None)

            # write to batabase BD_RISK_CROSS_TRADE_WARAIN
            Trade_Data = chemical_result_cp.rename(columns={'BIZOP_ETPS_SCCD': 'CORP_CREDIT_CODE','CUSTOM_ID':'CUSTOMS_CODE','BIZOP_ETPS_NM': 'CORP_NAME','RLT_ENTRY_NO':'BUSINESS_NO','INVT_IOCHKPT_STUCD':'STATUS','ACCESS_TIME_DIFF':'DURATION'})
            Trade_list = ['ID', 'CUSTOMS_CODE', 'CORP_CREDIT_CODE', 'CORP_NAME', 'TRADE_CODE', 'BUSINESS_NO', 'LABEL','DURATION','WH_REC_PREENT_NO', 'ACCESS_TIME', 'STATUS']
            Trade_Data = Trade_Data[Trade_list]
            Trade_Data['LABEL'] = ''
            Trade_Data['TYPE_FIRST'] = 'GNYTJKYJ'
            Trade_Data['TYPE_SECOND'] = 'BSWL'
            Trade_Data['BUSINESS_TYPE'] = '62'
            Trade_Data['ORDER_TYPE'] = 'entry'
            Trade_Data['DESCRIBE'] = ''
            Trade_Data['CONTEXT'] = Trade_Data.loc[:,['STATUS', 'ACCESS_TIME','WH_REC_PREENT_NO','DURATION']].apply(
                                        lambda x: json.dumps({'status': x['STATUS'],'accessTime': str(x['ACCESS_TIME']),
                                                              'whRecNo': x['WH_REC_PREENT_NO'],"duration":str(x['DURATION'])},
                                                             ensure_ascii=False),axis = 1)
            Trade_Data['RESOLVE_STATUS'] = str(0)
            Trade_Data['CREATE_TIME'] = now_date
            Trade_Data['UPDATE_TIME'] = now_date
            Trade_Data['RISK_LEVEL'] = str(3)
            Trade_Data['RESOLVE_START_DATE'] = ''
            Trade_Data['RESOLVE_END_DATE'] = ''
            Trade_Data['RLT_ID'] = ''
            Trade_Data['ext'] = ''
            Trade_Data['ext1'] = ''
            Trade_Data['ext2'] = ''
            order_list = ['ID', 'CUSTOMS_CODE', 'CORP_CREDIT_CODE', 'CORP_NAME', 'TRADE_CODE','TYPE_FIRST','TYPE_SECOND','BUSINESS_TYPE','ORDER_TYPE',
                          'BUSINESS_NO','LABEL','DESCRIBE','CONTEXT','RESOLVE_STATUS','CREATE_TIME','UPDATE_TIME','RISK_LEVEL','RESOLVE_START_DATE',
                          'RESOLVE_END_DATE','RLT_ID','ext','ext1','ext2']
            Trade_Data = Trade_Data[order_list]
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_WARAIN_TEMP', Trade_Data, None,alarm=None)


    def run_model_CHEMICAL(self):
        try:
            self.model_CHEMICAL()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            print(1)
            ##Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()

if __name__ == '__main__':
        # 读取传入的 child_task_id，仅适用于py调用,先备注掉
        if params_global.is_test:
            child_task_id = 'd03c492250e344b79d1ef9453d47f4ea'
        else:
            child_task_id = sys.argv[1]
        #org_code, param_json, base_time = read_log_table(child_task_id)
        org_code = None
        ChemicalModel(child_task_id, org_code).run_model_CHEMICAL()

