import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\WorkSpace\Pycharm_Space\bdrisk_model\risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class WareHouseWeightModel(object):
    def __init__(self,org_code,child_task_id,params):
        self.org_code = org_code
        self.child_task_id = child_task_id
        self.GROSS_WT_AVG = 2
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)


    def model_wht(self):

        sql = """ SELECT * FROM {} WHERE iscurrent =1 AND GROSS_WT_AVG > {}""".format('DW_CUS_RC.BD_RISK_CROSS_TRADE_WAREHOUSE_WEIGHT_CLEAN',self.GROSS_WT_AVG)
        result_data = Read_Oracle().read_oracle(sql=sql, database='dbdw')

        if result_data.empty:
            result_data = pd.DataFrame()
        else:
            result_data['LABEL'] = '库存重量与面积比例异常'
            Result_Data_cp = result_data.copy(deep = True)
            need_list = ['CREDIT_CODE','ORG_ID','AREA_USAGE','SUTTLE_TOTAL','GROSS_WT_AVG','BUSINESS_START_TIME','BUSINESS_END_TIME',
                         'LABEL']
            result_data = result_data[need_list]
            result_data = result_data.reset_index().rename(columns={'index': 'ID'})
            result_data['SCORE'] = 0
            result_data['MODEL_TIME'] = datetime.datetime.now()
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_WAREHOUS_WT_WARA', result_data, None,alarm=None)

            ####写入预警汇总
            #######将org_id 改成 org_code
            need_list_cp = ['CUSTOMS_CODE','CREDIT_CODE','TRADE_NAME','TRADE_CODE','ORG_ID', 'SUTTLE_TOTAL', 'GROSS_WT_AVG', 'BUSINESS_START_TIME',
                            'BUSINESS_END_TIME', 'AREA_USAGE', 'LABEL']
            Result_Data_cp = Result_Data_cp[need_list_cp]
            Result_Data_cp = Result_Data_cp.reset_index().rename(columns={'index': 'ID'})
            Result_Data_cp = Result_Data_cp.rename(
                columns={'CREDIT_CODE': 'CORP_CREDIT_CODE', 'TRADE_NAME': 'CORP_NAME','ORG_ID': 'BUSINESS_NO'})
            Result_Data_cp['AREA_USAGE'] = (Result_Data_cp['AREA_USAGE'].map(float)) * 100
            Result_Data_cp['TYPE_FIRST'] = 'GNYTJKYJ'
            Result_Data_cp['TYPE_SECOND'] = 'BSWL'
            Result_Data_cp['BUSINESS_TYPE'] = '65'
            Result_Data_cp['ORDER_TYPE'] = 'org'
            Result_Data_cp['DESCRIBE'] = ''
            Result_Data_cp['CONTEXT'] = Result_Data_cp.loc[:, ['AREA_USAGE', 'SUTTLE_TOTAL', 'GROSS_WT_AVG','BUSINESS_START_TIME','BUSINESS_END_TIME']].apply(
                lambda x: json.dumps({'areaUsage': x['AREA_USAGE'], 'suttleTotal': x['SUTTLE_TOTAL'],
                                      'grossWtAvg': x['GROSS_WT_AVG'], "businessStartTime": str(x['BUSINESS_START_TIME']),
                                      "businessEndTime": str(x['BUSINESS_END_TIME'])},ensure_ascii=False), axis=1)

            Result_Data_cp['RESOLVE_STATUS'] = str(0)
            Result_Data_cp['CREATE_TIME'] = datetime.datetime.now()
            Result_Data_cp['UPDATE_TIME'] = datetime.datetime.now()
            Result_Data_cp['RISK_LEVEL'] = str(3)
            Result_Data_cp['RESOLVE_START_DATE'] = ''
            Result_Data_cp['RESOLVE_END_DATE'] = ''
            Result_Data_cp['RLT_ID'] = ''
            Result_Data_cp['LABEL'] = ''
            Result_Data_cp['ext'] = ''
            Result_Data_cp['ext1'] = ''
            Result_Data_cp['ext2'] = ''

            order_list = ['ID', 'CUSTOMS_CODE', 'CORP_CREDIT_CODE', 'CORP_NAME', 'TRADE_CODE', 'TYPE_FIRST', 'TYPE_SECOND',
                          'BUSINESS_TYPE', 'ORDER_TYPE',
                          'BUSINESS_NO', 'LABEL', 'DESCRIBE', 'CONTEXT', 'RESOLVE_STATUS', 'CREATE_TIME', 'UPDATE_TIME',
                          'RISK_LEVEL', 'RESOLVE_START_DATE',
                          'RESOLVE_END_DATE', 'RLT_ID', 'ext', 'ext1', 'ext2']
            Result_Data_cp = Result_Data_cp[order_list]
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_WARAIN_TEMP', Result_Data_cp, None, alarm=None)



    def run_model_wht(self):
        exec_status = None
        try:
            self.model_wht()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()

if __name__ == '__main__':
        # 读取传入的 child_task_id，仅适用于py调用,先备注掉
        child_task_id = sys.argv[1]
        #child_task_id = 'childtaskidtd1001'
        #param_json = None
        org_code, param_json, base_time = read_log_table(child_task_id)
        org_code = None
        WareHouseWeightModel(org_code,child_task_id,param_json).run_model_wht()



