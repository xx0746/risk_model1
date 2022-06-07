import sys, os
from os import path
import datetime
if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk-model\risk_model\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
    print(path.dirname(path.dirname(os.getcwd())))
from risk_models import *
pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth',1000)

class Origin_Country_Logic(object):
    def __init__(self, org_code, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time

    def deal_comparison_dic(self, df):
        temp_dic = {}
        str = "{"
        temp_dic['code_t'] = df['G_NAME']
        temp_dic['qty1'] = df['G_QTY']
        temp_dic['originCountry'] = df['ORIGIN_COUNTRY']
        temp_dic['codeTCompare'] = df['G_NAME_COMPARE']
        temp_dic['qty1Compare'] = df['G_QTY_COMPARE']
        temp_dic['originCountryCompare'] = df['ORIGIN_COUNTRY_COMPARE']
        for key, value in temp_dic.items():
            str += "\"%s\":\"%s\"," % (key, value)
        str = str[:-1]
        return str + "},"

    def deal_entry_dic(self, df):
        temp_dic = {}
        str = "{"
        temp_dic['entryId'] = df['ENTRY_ID']
        temp_dic['entryIdCompare'] = df['ENTRY_ID_COMPARE']
        temp_dic['comparison'] = df['COMPARISON']
        temp_dic['ownerCode'] = df['CONSIGN_SCC']
        temp_dic['ownerName'] = df['CONSIGN_NAME']
        temp_dic['ownerCodeCompare'] = df['CONSIGN_SCC_COMPARE']
        temp_dic['ownerNameCompare'] = df['CONSIGN_NAME_COMPARE']
        for key, value in temp_dic.items():
            str += "\"%s\":\"%s\"," % (key, value)
        str = str[:-1]
        return str + "},"

    def deal_context_dic(self, df):
        temp_dic = {}
        str = "{"
        temp_dic['businessStartTime'] = datetime.datetime.now()
        temp_dic['businessEndTime'] = datetime.datetime.now()
        temp_dic['entry'] = df['entry']
        for key, value in temp_dic.items():
            str += "\"%s\":\"%s\"," % (key, value)
        str = str[:-1]
        return str + "}"

    def logic_layer(self):
        sql_text = f'''
        select 
        t1.ID, 
        t1.ENTRY_ID, 
        t1.I_E_PORT, 
        t1.D_DATE, 
        t1.CONSIGN_SCC,
        t1.CONSIGN_CODE,
        t1.CONSIGN_NAME,
        t1.OWNER_CODE_SCC as CORP_CREDIT_CODE,
        t1.OWNER_CODE_SCC as BUSINESS_NO, 
        t1.OWNER_CODE, 
        t1.OWNER_NAME, 
        t1.TRADE_MODE, 
        t1.G_NAME, 
        t1.G_QTY, 
        t1.ORIGIN_COUNTRY,
        t2.ENTRY_ID as ENTRY_ID_COMPARE, 
        t2.I_E_PORT as I_E_PORT_COMPARE, 
        t2.D_DATE as D_DATE_COMPARE, 
        t2.OWNER_CODE_SCC as OWNER_CODE_SCC_COMPARE, 
        t2.OWNER_CODE as OWNER_CODE_COMPARE,
        t2.OWNER_NAME as OWNER_NAME_COMPARE,
        t2.TRADE_MODE as TRADE_MODE_COMPARE, 
        t2.CONSIGN_SCC AS CONSIGN_SCC_COMPARE,
        t2.CONSIGN_CODE AS CONSIGN_CODE_COMPARE,
        t2.CONSIGN_NAME AS CONSIGN_NAME_COMPARE,
        t2.G_NAME as G_NAME_COMPARE, 
        t2.G_QTY as G_QTY_COMPARE, 
        t2.ORIGIN_COUNTRY as ORIGIN_COUNTRY_COMPARE 
        from DW_CUS_RC.BD_RISK_CROSS_TRADE_RESULT_ORIGIN_COUNTRY_DIFFER_CLEAN t1 
        join DW_CUS_RC.BD_RISK_CROSS_TRADE_RESULT_ORIGIN_COUNTRY_DIFFER_CLEAN t2 
        on t1.OWNER_CODE_SCC = t2.OWNER_CODE_SCC 
        and t1.G_NAME = t2.G_NAME 
        and t1.G_QTY = t2.G_QTY 
        and t1.ORIGIN_COUNTRY != t2.ORIGIN_COUNTRY 
        and t1.TRADE_MODE != t2.TRADE_MODE
                    '''
        origin_country_df = Read_Oracle().read_oracle(sql=sql_text, database='dbalarm')
        if origin_country_df is not None and len(origin_country_df) != 0:
            origin_country_df['COMPARISON'] = origin_country_df.apply(lambda x: self.deal_comparison_dic(x), axis=1)
            origin_country_df = origin_country_df.groupby(['ENTRY_ID', 'CORP_CREDIT_CODE', 'BUSINESS_NO', 'ENTRY_ID_COMPARE']).agg({'COMPARISON': 'sum', 'ENTRY_ID': 'first', 'ENTRY_ID_COMPARE': 'first', 'CORP_CREDIT_CODE': 'first', 'OWNER_CODE': 'first', 'OWNER_NAME': 'first', 'OWNER_CODE_SCC_COMPARE': 'first', 'OWNER_CODE_COMPARE': 'first', 'OWNER_NAME_COMPARE': 'first', 'CONSIGN_SCC': 'first', 'CONSIGN_SCC_COMPARE': 'first', 'CONSIGN_NAME': 'first', 'CONSIGN_NAME_COMPARE': 'first',})
            origin_country_df['COMPARISON'] = origin_country_df['COMPARISON'].map(lambda x: "[" + x[: -1] + "]")
            origin_country_df['entry'] = origin_country_df.apply(lambda x: self.deal_entry_dic(x), axis=1)
            origin_country_df.drop(columns=['CORP_CREDIT_CODE'], inplace=True)

            # entry_df = origin_country_df.groupby(['CORP_CREDIT_CODE', 'BUSINESS_NO']).agg({'entry': 'sum',  'OWNER_CODE': 'first', 'OWNER_NAME': 'first'})
            entry_df = origin_country_df.groupby(['CORP_CREDIT_CODE', 'BUSINESS_NO']).agg({'entry': 'first', 'OWNER_CODE': 'first', 'OWNER_NAME': 'first'})
            entry_df['entry'] = entry_df['entry'].map(lambda x: "[" + x[: -1] + "]")
            entry_df['CONTEXT'] = entry_df.apply(lambda x: self.deal_context_dic(x), axis=1)
            entry_df['ID'] = 0
            entry_df["CUSTOMS_CODE"] = "2249"
            entry_df["TYPE_FIRST"] = "GNYTJKYJ"
            entry_df["TYPE_SECOND"] = "BSWL"
            entry_df["BUSINESS_TYPE"] = "61"
            entry_df["ORDER_TYPE"] = "trade"
            entry_df["RESOLVE_STATUS"] = "0"
            entry_df["RISK_LEVEL"] = "3"
            # entry_df["LABEL"] = "一二线原产国预警"
            entry_df["TRADE_CODE"] = entry_df["OWNER_CODE"]
            entry_df["CORP_NAME"] = entry_df["OWNER_NAME"]
            entry_df.drop(columns=['entry', 'OWNER_NAME', 'OWNER_CODE'], inplace=True)
            entry_df = entry_df.reset_index()
            print(entry_df)

            if entry_df is not None:
                Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_WARAIN_TEMP', entry_df, org_code=None, alarm=None)

    def run_logic_layer(self):
        try:
            self.logic_layer()
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
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉 /
    if params_global.is_test:
        child_task_id = 'Origin_Country_Differ_LOGIC'
    else:
        child_task_id = sys.argv[1]
    # org_code, param_json, base_time = read_log_table(child_task_id)
    Origin_Country_Logic(None, None, child_task_id).run_logic_layer()