
import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"F:/git_bdrisk/bdrisk-model/risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class ChemicalClean(object):
    def __init__(self,org_code,child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status = None)
        ##param
        self.org_code = org_code
        self.now_date = datetime.datetime.now()


    def clean_Chemical(self):
        ###get_
        # Read WAREHOUSE_RECEIPT_BSC table
        sql = """SELECT DISTINCT a.WH_REC_PREENT_NO,a.BIZOP_ETPS_NM,a.BIZOP_ETPS_SCCD,c.CUSTOM_ID,c.TRADE_CODE,a.BUSINESS_TYPECD,a.INVT_IOCHKPT_STUCD,a.IE_TYPECD,a.RLT_ENTRY_NO,b.ACCESS_TIME,c.LABEL_CODE   FROM {} a join {} b  ON a.WH_REC_PREENT_NO = b.WH_REC_PREENT_NO JOIN {} c ON  a.BIZOP_ETPS_SCCD = c.CREDIT_CODE
                 WHERE a.WH_REC_PREENT_NO IS NOT NULL AND a.RLT_ENTRY_NO IS NOT NULL AND IE_TYPECD = 'E'  AND INVT_IOCHKPT_STUCD <> '3' AND a.IN_EXP_TYPE not in ('1','4','8','7') AND  a.MASTER_CUSCD = '2249' AND b.ACCESS_TIME IS NOT NULL AND c.LABEL_CODE LIKE '%003%' """.format('LGSA.WAREHOUSE_RECEIPT_BSC', 'LGSA.WAREHOUSE_ENTRY','N_EPZ_CUS.TRADE')
        wrb_data = Read_Oracle().read_oracle(sql=sql, database='dblgsa')

        ## if data empty , stop proceeding
        if wrb_data.empty :
            print(self.org_code + ':no data records')
            result_df = wrb_data
        else:
            for idx, data_del in wrb_data.iterrows():
                    ACCESS_TIME_DIFF = (self.now_date - data_del['ACCESS_TIME']).days
                    wrb_data.loc[idx,'ACCESS_TIME_DIFF'] = ACCESS_TIME_DIFF
                    wrb_data.loc[idx, 'LABEL_CODE_Infor'] = '1' if '003' in data_del['LABEL_CODE'].split(',') else '0'
            wrb_data = wrb_data[wrb_data['LABEL_CODE_Infor'] == '1']
            ###get need data
            need_list = ['BIZOP_ETPS_NM','WH_REC_PREENT_NO','BIZOP_ETPS_SCCD','CUSTOM_ID','TRADE_CODE','BUSINESS_TYPECD','INVT_IOCHKPT_STUCD','ACCESS_TIME','RLT_ENTRY_NO','ACCESS_TIME_DIFF']
            wrb_data = wrb_data[need_list]
            wrb_data['CHECK_TIME'] = self.now_date
            wrb_data = wrb_data.reset_index().rename(columns={'index': 'ID'})

            ## if len(list_result) == 0 , stop proceeding
            if wrb_data.empty :
                print(self.org_code+':no data records')
            else:
                Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_DETAIL_CHEMICAL_CLEAN', wrb_data,None,alarm=None)



    def run_clean_Chemical(self):
        try:
            self.clean_Chemical()
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
    if params_global.is_test:
        child_task_id = '1ed78494e4b54b5f8d13521a0d417636'
    else:
        child_task_id = sys.argv[1]
    # org_code,base_time = read_log_table(child_task_id)
    org_code = None
    ChemicalClean(org_code,child_task_id).run_clean_Chemical()




