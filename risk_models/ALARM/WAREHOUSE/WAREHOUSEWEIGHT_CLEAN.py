import sys, os
from os import path


if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\WorkSpace\Pycharm_Space\bdrisk_model\risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class WareHouseWeightClean(object):

    def __init__(self,org_code,child_task_id,start_time,end_time,AREA_USAGE):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status = None)
        ##param
        self.org_code = org_code
        self.AREA_USAGE = AREA_USAGE
        self.start_time = start_time ### Query start time
        self.end_time = end_time ###query end time

         ####模型运行时间
        self.now = datetime.datetime.now()
        self.month_start = datetime.datetime(self.now.year, self.now.month, 1)  ###上个月最后一天时间



    ########获得自定义时间区间内的数据
    def get_data_self_defined(self):
        ###需要加一个判断
        sql = """ SELECT DISTINCT a.CREDIT_CODE,b.ORG_CODE ORG_ID,d.WH_REC_PREENT_NO,d.IE_TYPECD,d.DCL_TIME,d.GROSS_WT FROM {} a JOIN {} b ON a.id  = b.TRADE_ID JOIN {} c ON b.id = c.ORG_ID JOIN {} d ON c.ORG_ID  = d.ORG_ID 
                  WHERE b.ORG_CODE IS NOT NULL AND d.DCL_TIME < to_date('{}','yyyy-mm-dd hh24:mi:ss') and d.DCL_TIME > to_date('{}','yyyy-mm-dd hh24:mi:ss')""".format('N_EPZ_CUS.Trade','LGSA.Organization',
                                                                     'LGSA.Organization_E','LGSA.WAREHOUSE_RECEIPT_BSC',
                                                                      self.end_time, self.start_time)

        WEIGTH_DE_data = Read_Oracle().read_oracle(sql=sql, database='dbods')

        return WEIGTH_DE_data

    ####获得企业信息
    def get_trade_infor(self):
        sql = """ SELECT DISTINCT a.id,a.TRADE_NAME,a.CREDIT_CODE,a.TRADE_CODE,b.ORG_CODE ORG_ID,a.CUSTOM_ID CUSTOMS_CODE,c.AREA FROM {} a JOIN {} b ON a.id  = b.TRADE_ID JOIN {} c ON b.id = c.ORG_ID 
                  WHERE  b.ORG_CODE IS NOT NULL AND c.AREA IS NOT NULL """.format('N_EPZ_CUS.Trade','LGSA.Organization','LGSA.Organization_E')

        trade_inf = Read_Oracle().read_oracle(sql=sql, database='dbods')

        return trade_inf

    ###区分进出口计算货物净重
    def Calculation_Data(self,df_data,trade_inf):
            ###group by area,IE_TYPECD,ORG_ID sum(GROSS_WT)
            trade_inf2 = trade_inf[['CREDIT_CODE','ORG_ID']]
            Gruop_data = df_data.groupby(by=['IE_TYPECD', 'ORG_ID'], as_index=False)['GROSS_WT'].sum()
            i_df = Gruop_data[Gruop_data['IE_TYPECD'] == 'I'].drop('IE_TYPECD', axis=1)
            i_df.columns = ['ORG_ID', 'GROSS_WT_I']
            i_df = pd.merge(trade_inf2, i_df, on='ORG_ID', how='left')
            e_df = Gruop_data[Gruop_data['IE_TYPECD'] == 'E'].drop('IE_TYPECD', axis=1)
            e_df.columns = ['ORG_ID', 'GROSS_WT_E']
            e_df = pd.merge(trade_inf2, e_df, on='ORG_ID', how='left')
            e_i_df = pd.merge(i_df, e_df, on='ORG_ID', how='inner')
            e_i_df = e_i_df.drop(['CREDIT_CODE_y','CREDIT_CODE_x'],axis=1)
            # get suttle
            e_i_df = e_i_df.fillna(0) ### fill null
            e_i_df['BUSINESS_START_TIME'] = (df_data['BUSINESS_START_TIME'].values)[0]
            e_i_df['SUTTLE_TOTAL'] = e_i_df['GROSS_WT_I'] - e_i_df['GROSS_WT_E']
            return e_i_df


    ###计算每平米货物重量
    def Calculation_avg_Data(self,df_data):
        df_data = df_data.fillna(0) ###ORG_ID 为 库存为O的填充
        df_data['AREA'] = df_data['AREA'].map(float)
        df_data['SUTTLE_TOTAL'] = df_data['SUTTLE_TOTAL'].map(float)
        df_data['GROSS_WT_AVG'] = df_data['SUTTLE_TOTAL']/df_data['AREA']
        df_data['GROSS_WT_AVG'] = df_data['GROSS_WT_AVG']/ float(self.AREA_USAGE)
        df_data['AREA_USAGE'] = round(float(self.AREA_USAGE),2)
        return df_data

    ####获得历史数据 读取 历史净重 时间
    def get_History_WT(self):
        sql_history = """ SELECT * FROM {}  WHERE  ISCURRENT = 1 """.format('DW_CUS_RC.BD_RISK_CROSS_TRADE_WAREHOUSE_WEIGHT_CLEAN')
        WEIGTH_history_data = Read_Oracle().read_oracle(sql=sql_history, database='dbdw')
        return WEIGTH_history_data

    ####获得当前时期的货物数据
    def get_new_data(self,history_data):

        if history_data.empty:###为空 无数据
            start_data = datetime.datetime.strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        else:
            start_data = history_data['BUSINESS_END_TIME'][0]

        sql = """ SELECT DISTINCT a.CREDIT_CODE,b.ORG_CODE ORG_ID,d.WH_REC_PREENT_NO,d.IE_TYPECD,d.DCL_TIME,d.GROSS_WT FROM {} a JOIN {} b ON a.id  = b.TRADE_ID JOIN {} c ON b.id = c.ORG_ID JOIN {} d ON c.ORG_ID  = d.ORG_ID                                               
                                               WHERE b.ORG_CODE IS NOT NULL and d.DCL_TIME >= to_date('{}','yyyy-mm-dd hh24:mi:ss')
                                                and d.DCL_TIME < to_date('{}','yyyy-mm-dd hh24:mi:ss') and d.DCL_TIME is not null """.format(
                                                'N_EPZ_CUS.Trade', 'LGSA.Organization','LGSA.Organization_E','LGSA.WAREHOUSE_RECEIPT_BSC',start_data,self.month_start)


        WEIGTH_data = Read_Oracle().read_oracle(sql=sql, database='dbods')

        if WEIGTH_data.empty:
            WEIGTH_data = pd.DataFrame()
        else:
            WEIGTH_data['BUSINESS_START_TIME'] = start_data
        return WEIGTH_data

    ###当前与历史数据合并
    def combine_new_history(self,date_trde,history_data):
        history_weigth = history_data[['ORG_ID','SUTTLE_TOTAL']] ###获得历史净重
        history_weigth.columns = ['ORG_ID', 'GROSS_WT_HSY']
        combine_df = pd.merge(date_trde,history_weigth, on='ORG_ID', how='left') ###过去和历史结果表
        combine_df['SUTTLE_TOTAL'] = combine_df['SUTTLE_TOTAL'].map(float)
        combine_df['GROSS_WT_HSY'] = combine_df['GROSS_WT_HSY'].map(float)
        combine_df['SUTTLE_TOTAL'] = combine_df['SUTTLE_TOTAL'] + combine_df['GROSS_WT_HSY']
        combine_df = combine_df.fillna(0)
        return combine_df

    def clean_WareHouseWeight(self):
        ###get trade infor
        trade_inf = self.get_trade_infor()
        ####根据传入的时间参数判断是否是自定义查询时间
        if self.start_time is None:  ###不是自定义事件
            history_data = self.get_History_WT()
            if history_data.empty: ###为空 无历史数据拉全量
                new_data = self.get_new_data(history_data) ###获得全部数据
                if new_data.empty: ###读的数据为空不用合并 直接返回
                    result_df = pd.DataFrame()
                else:
                    new_data_cd = self.Calculation_Data(new_data,trade_inf) ###合并计算
                    date_trde = pd.merge(trade_inf, new_data_cd, on='ORG_ID', how='left')  ###gei the area
                    result_df = self.Calculation_avg_Data(date_trde) ###获得单位面积重量
            else: ####不为空
                new_data = self.get_new_data(history_data)  ###获得全部数据
                if new_data.empty:##新一周期数据没读到返回历史数据为最新数据
                    result_df = history_data
                else: ###读的新周期数据不为空 合并计算
                    #new_data = self.get_new_data(history_data)  ###获得全部数据
                    new_data_cd = self.Calculation_Data(new_data,trade_inf) ###数据计算
                    date_trde = pd.merge(trade_inf, new_data_cd, on='ORG_ID', how='left')  ###gei the area
                    comb_df = self.combine_new_history(date_trde,history_data)###与历史数据合并
                    result_df = self.Calculation_avg_Data(comb_df)
        else: ###自定义从查询时间
            self_date = self.get_data_self_defined()
            ##选择有效数据
            if self_date.empty:
                print(self.org_code + ':no data records')
            else:
                self_date['BUSINESS_START_TIME'] =self.start_time
                elf_df_clean = self.Calculation_Data(self_date,trade_inf)
                sefl_date_trde = pd.merge(trade_inf,elf_df_clean,on='ORG_ID', how='left')
                result_df = self.Calculation_avg_Data(sefl_date_trde)




        if result_df.empty :
            print(self.org_code + ':no data records')
        else: ###不为空 读取最后存储数据格式
            need_list = ['CUSTOMS_CODE','TRADE_NAME','CREDIT_CODE','TRADE_CODE','ORG_ID','AREA','AREA_USAGE','SUTTLE_TOTAL','GROSS_WT_E','GROSS_WT_I','GROSS_WT_AVG','BUSINESS_START_TIME']
            result_df = result_df[need_list]
            result_df = result_df.reset_index().rename(columns={'index': 'ID'})
            result_df['BUSINESS_END_TIME'] = self.month_start  ###模型运行时间月份第一天
            result_df['MODEL_TIME'] = self.now
            Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_WAREHOUSE_WEIGHT_CLEAN', result_df,None,alarm=None)


    def run_clean_WareHouse(self):
        try:
            self.clean_WareHouseWeight()
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
        org_code,param_json,BASE_TIME = read_log_table(child_task_id)
    WareHouseWeightClean(None,child_task_id,None,None,0.7).run_clean_WareHouse()







