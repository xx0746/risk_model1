import sys, os
from os import path
if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"F:/bdrisk_model/risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *

class ScoreTt(object):
    def __init__(self, child_task_id, org_code, params):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status=None)
        self.database = "dbalarm" if params_global.is_test else "dbdw"
        # 参数读取
        self.org_code = org_code
        # 填充缺失的分数，默认为100
        js_para = json.loads(params)
        if 'fillna_score' in js_para:
            self.fillna_score = js_para['fillna_score']

    def score_total(self):
        model_code = 'TOTAL'
        #计算新的总分
        #读取企业小分
        p0 = f'''
        select ORG_CODE,HG_ORG_ID,MODEL_CODE,SCORE from BD_RISK_CROSS_TRADE_SCORE 
        WHERE ISCURRENT = 1 
        AND MODEL_CODE IN ('SUPERVISOR','INNOVATION','CREDIT','LOGISTIC','THROUGH_CUSTOM','SUPPLY_CHAIN','STOCK','PUBLIC','PRODUCTION','FINANCE') 
        AND BUSINESS_TIME = trunc(add_months(sysdate,-1),'mm')  '''
        score_list = Read_Oracle().read_oracle(sql=p0, database=self.database)

        if len(score_list) == 0:
            print(f'Error: 未取到上月的分数数据.')
            return

        sql=f"""\
        select DISTINCT ORG_CODE,TRADE_CODE,LABEL_CODE from BD_RISK_CORP_INFO_BASIC WHERE ISCURRENT = 1
        """
        corp_list = Read_Oracle().read_oracle(sql=sql, database=self.database)
        if len(corp_list) == 0:
            print('ERROR: 未获取到任何企业信息')
            return
        corp_list.columns = ['ORG_CODE','HG_ORG_ID','LABEL_CODE']
        corp_list['ORG_TYPE'] = corp_list['LABEL_CODE'].apply(lambda x: x.split(sep=',')[0])
        corp_list = corp_list[['ORG_CODE','HG_ORG_ID','ORG_TYPE']].copy()
        corp_list_cp = corp_list.copy()

        corp_list.loc[corp_list['ORG_CODE']=='91310000132612172J',['ORG_TYPE']] = '91310000132612172J'

        #处理小分，如有缺失用参数中提供的默认值来补
        model_list = ['SUPERVISOR','INNOVATION','CREDIT','LOGISTIC','THROUGH_CUSTOM','SUPPLY_CHAIN','STOCK','PUBLIC','PRODUCTION','FINANCE']
        df_model = pd.DataFrame(model_list,columns=['MODEL_CODE'])
        score_result = pd.merge(df_model,score_list,how='left',on=['MODEL_CODE'])
        score_result['SCORE'].fillna(self.fillna_score,inplace=True)

        # 补充企业类型信息
        score_result = pd.merge(score_result,corp_list,how = 'inner',on = ['ORG_CODE','HG_ORG_ID'])


        #读分数json
        # 保税加工
        sql=f"""\
        SELECT WEIGHT FROM PARAM_VALUE_CONFIG WHERE BUS_SON_TYPE = 'crossTradeBigData->EnterpriseScore->maquiladora' and CUSCD = '2249'
        """
        param_data = Read_Oracle().read_oracle(sql=sql, database='dblgsa')

        if len(param_data) == 0:
            print('ERROR: 未获取到保税加工权重信息')
            return
        else:
            params = param_data.loc[0,'WEIGHT']

        js_para = json.loads(params)
        para_data1 = {'MODEL_CODE':['INNOVATION','PRODUCTION','PUBLIC','SUPPLY_CHAIN','SUPERVISOR','STOCK','THROUGH_CUSTOM','FINANCE','LOGISTIC','CREDIT'],
                    'WEIGHT':[js_para.get('Model_A'),js_para.get('Model_B'),js_para.get('Model_C'),js_para.get('Model_D'),js_para.get('Model_E'),
                            js_para.get('Model_F'),js_para.get('Model_G'),js_para.get('Model_H'),js_para.get('Model_I'),js_para.get('Model_J')]}
        df_para1 = pd.DataFrame(para_data1,index=range(10))
        df_para1['WEIGHT'] = df_para1['WEIGHT'].fillna(0)
        df_para1['WEIGHT'] = df_para1['WEIGHT'].astype(float)
        df_para1['ORG_TYPE'] = '002'
        sum_para1 = df_para1['WEIGHT'].sum()

        if sum_para1!=100:
            print('ERROR: 保税加工权重之和不为100')
            return

        # 保税服务
        sql=f"""\
        SELECT WEIGHT FROM PARAM_VALUE_CONFIG WHERE BUS_SON_TYPE = 'crossTradeBigData->EnterpriseScore->Bondedservices' and CUSCD = '2249'
        """
        param_data = Read_Oracle().read_oracle(sql=sql, database='dblgsa')

        if len(param_data) == 0:
            print('ERROR: 未获取到保税服务权重信息')
            return
        else:
            params = param_data.loc[0,'WEIGHT']

        js_para = json.loads(params)
        para_data2 = {'MODEL_CODE':['INNOVATION','PRODUCTION','PUBLIC','SUPPLY_CHAIN','SUPERVISOR','STOCK','THROUGH_CUSTOM','FINANCE','LOGISTIC','CREDIT'],
                    'WEIGHT':[js_para.get('Model_A'),js_para.get('Model_B'),js_para.get('Model_C'),js_para.get('Model_D'),js_para.get('Model_E'),
                            js_para.get('Model_F'),js_para.get('Model_G'),js_para.get('Model_H'),js_para.get('Model_I'),js_para.get('Model_J')]}
        df_para2 = pd.DataFrame(para_data2,index=range(10))
        df_para2['WEIGHT'] = df_para2['WEIGHT'].fillna(0)
        df_para2['WEIGHT'] = df_para2['WEIGHT'].astype(float)
        df_para2['ORG_TYPE'] = '009'
        sum_para2 = df_para2['WEIGHT'].sum()

        if sum_para2!=100:
            print('ERROR: 保税服务权重之和不为100')
            return

        # 保税物流
        sql=f"""\
        SELECT WEIGHT FROM PARAM_VALUE_CONFIG WHERE BUS_SON_TYPE = 'crossTradeBigData->EnterpriseScore->BondedLogistic' and CUSCD = '2249'
        """
        param_data = Read_Oracle().read_oracle(sql=sql, database='dblgsa')

        if len(param_data) == 0:
            print('ERROR: 未获取到保税物流权重信息')
            return
        else:
            params = param_data.loc[0,'WEIGHT']

        js_para = json.loads(params)
        para_data3 = {'MODEL_CODE':['INNOVATION','PRODUCTION','PUBLIC','SUPPLY_CHAIN','SUPERVISOR','STOCK','THROUGH_CUSTOM','FINANCE','LOGISTIC','CREDIT'],
                    'WEIGHT':[js_para.get('Model_A'),js_para.get('Model_B'),js_para.get('Model_C'),js_para.get('Model_D'),js_para.get('Model_E'),
                            js_para.get('Model_F'),js_para.get('Model_G'),js_para.get('Model_H'),js_para.get('Model_I'),js_para.get('Model_J')]}
        df_para3 = pd.DataFrame(para_data3,index=range(10))
        df_para3['WEIGHT'] = df_para3['WEIGHT'].fillna(0)
        df_para3['WEIGHT'] = df_para3['WEIGHT'].astype(float)
        df_para3['ORG_TYPE'] = '001'
        sum_para3 = df_para3['WEIGHT'].sum()

        if sum_para3!=100:
            print('ERROR: 保税物流权重之和不为100')
            return

        # 上飞
        sql = f"""
        SELECT WEIGHT FROM PARAM_VALUE_CONFIG WHERE BUS_SON_TYPE = 'crossTradeBigData->EnterpriseScore->SF' and CUSCD = '2249'
        """
        param_data = Read_Oracle().read_oracle(sql=sql, database='dblgsa')

        if len(param_data) == 0:
            print('ERROR: 未获取到上飞总分权重信息')
            return
        else:
            params = param_data.loc[0,'WEIGHT']

        js_para = json.loads(params)
        para_data4 = {'MODEL_CODE':['INNOVATION','PRODUCTION','PUBLIC','SUPPLY_CHAIN','SUPERVISOR','STOCK','THROUGH_CUSTOM','FINANCE','LOGISTIC','CREDIT'],
                    'WEIGHT':[js_para.get('Model_A'),js_para.get('Model_B'),js_para.get('Model_C'),js_para.get('Model_D'),js_para.get('Model_E'),
                            js_para.get('Model_F'),js_para.get('Model_G'),js_para.get('Model_H'),js_para.get('Model_I'),js_para.get('Model_J')]}
        df_para4 = pd.DataFrame(para_data4,index=range(10))
        df_para4['WEIGHT'] = df_para4['WEIGHT'].fillna(0)
        df_para4['WEIGHT'] = df_para4['WEIGHT'].astype(float)
        df_para4['ORG_TYPE'] = '91310000132612172J'
        sum_para4 = df_para4['WEIGHT'].sum()

        if sum_para4!=100:
            print('ERROR: 上飞总分权重之和不为100')
            return

        df_para = pd.concat([df_para1,df_para2,df_para3,df_para4])
        score_result = pd.merge(score_result, df_para, how='inner',on=['MODEL_CODE', 'ORG_TYPE'])
        score_result['SCORE'] = score_result['SCORE'].astype(float)
        score_result['WEIGHT'] = score_result['WEIGHT']/100
        score_result['WEIGHT_SCORE'] = score_result['SCORE'] * score_result['WEIGHT']

        score_res = score_result[['ORG_CODE','HG_ORG_ID','WEIGHT_SCORE']].copy()
        
        total_score_res = pd.DataFrame(score_res.groupby(['ORG_CODE', 'HG_ORG_ID'])['WEIGHT_SCORE'].sum())
        total_score_res = total_score_res.stack()
        total_score_res = total_score_res.reset_index()
        total_score_res.columns = ['ORG_CODE', 'HG_ORG_ID', 'NAME', 'SCORE']
        total_score_res = total_score_res[['ORG_CODE', 'HG_ORG_ID', 'SCORE']].copy()
        total_score_res = pd.merge(total_score_res,corp_list_cp,how='right',on = ['ORG_CODE', 'HG_ORG_ID'])
        total_score_res['SCORE'] = total_score_res['SCORE'].fillna(100)
        #保留2位小数
        total_score_res['SCORE'] = total_score_res['SCORE'].astype(float)
        total_score_res['SCORE'] = total_score_res['SCORE'].apply(lambda x: round(x,2))


        business_time = (datetime.date.today().replace(day=1) - datetime.timedelta(1)).replace(day=1)

        total_score_res['BUSINESS_TIME'] = business_time
        total_score_res['MODEL_CODE'] = model_code
        total_score_res['BUSINESS_TIME'] = total_score_res['BUSINESS_TIME'].astype('datetime64')
        total_score_res = total_score_res.reset_index(drop=True)
        total_score_res = total_score_res.reset_index().rename(columns={'index': 'ID'})

        Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_SCORE', total_score_res, org_code = None,alarm = [model_code,''])
        
    def run_score_total(self):
        try:
            self.score_total()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id = self.child_task_id, exec_status=exec_status).write_log()

if __name__ == '__main__':
    if params_global.is_test:
        child_task_id = sys.argv[1]
    else:
        child_task_id = sys.argv[1]
    org_code, params, base_time = read_log_table(child_task_id)
    # org_code = '91310000132612172J'
    # child_task_id, org_code, params
    ScoreTt(child_task_id, org_code, params).run_score_total()