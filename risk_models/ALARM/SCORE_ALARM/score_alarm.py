import sys, os
from os import path
if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk-model\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *

class ScoreAlarm(object):
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

    def score_alarm(self):
        #计算新的总分
        #读取企业小分
        p0 = f'''
        select ORG_CODE,MODEL_CODE,SCORE from BD_RISK_CROSS_TRADE_SCORE 
        WHERE ISCURRENT = 1 
        AND MODEL_CODE IN ('TOTAL','SUPERVISOR','INNOVATION','CREDIT','LOGISTIC','THROUGH_CUSTOM','SUPPLY_CHAIN','STOCK','PUBLIC','PRODUCTION','FINANCE') 
        AND BUSINESS_TIME = trunc(add_months(sysdate,-1),'mm')  '''
        score_list = Read_Oracle().read_oracle(sql=p0, database='dbalarm')

        if len(score_list) == 0:
            print(f'Error: 未取到上月的分数数据.')
            return
        score_list.columns = ['CORP_CREDIT_CODE','MODEL_CODE','SCORE']

        sql=f"""\
        select DISTINCT ORG_CODE, ORG_NAME, TRADE_CODE from BD_RISK_CORP_INFO_BASIC WHERE ISCURRENT = 1
        """
        corp_list = Read_Oracle().read_oracle(sql=sql, database='dbalarm')
        if len(corp_list) == 0:
            print('ERROR: 未获取到任何企业信息')
            return
        corp_list.columns = ['CORP_CREDIT_CODE','CORP_NAME','TRADE_CODE']

        #处理小分，如有缺失用参数中提供的默认值来补
        model_list = ['TOTAL','SUPERVISOR','INNOVATION','CREDIT','LOGISTIC','THROUGH_CUSTOM','SUPPLY_CHAIN','STOCK','PUBLIC','PRODUCTION','FINANCE']
        df_model = pd.DataFrame(model_list,columns=['MODEL_CODE'])
        score_result = pd.merge(df_model,score_list,how='left',on=['MODEL_CODE'])
        score_result['SCORE'].fillna(self.fillna_score,inplace=True)

        # 补充企业类型信息
        score_result = pd.merge(score_result,corp_list,how = 'inner',on = ['CORP_CREDIT_CODE'])

        # 分数预警
        sql=f"""\
        SELECT THRESHOLD FROM PARAM_VALUE_CONFIG WHERE BUS_SON_TYPE = 'crossTradeBigData->Portraits->BusPortraits' and CUSCD = '2249'
        """
        param_data = Read_Oracle().read_oracle(sql=sql, database='dblgsa')

        if len(param_data) == 0:
            print('ERROR: 未获取到分数预警阈值信息')
            return
        else:
            params = param_data.loc[0,'THRESHOLD']

        js_para = json.loads(params)
        para_data = {'MODEL_CODE':['INNOVATION','PRODUCTION','PUBLIC','SUPPLY_CHAIN','SUPERVISOR','STOCK','THROUGH_CUSTOM','FINANCE','LOGISTIC','CREDIT','TOTAL'],
                    'THRES':[js_para.get('INNOVATION'),js_para.get('PRODUCTION'),js_para.get('PUBLIC'),js_para.get('SUPPLY_CHAIN'),js_para.get('SUPERVISOR'),
                            js_para.get('STOCK'),js_para.get('THROUGH_CUSTOM'),js_para.get('FINANCE'),js_para.get('LOGISTIC'),js_para.get('CREDIT'),js_para.get('TOTAL')]}
        df_thres = pd.DataFrame(para_data,index=range(11))

        thres_result = pd.merge(score_result,df_thres,how = 'inner', on = ['MODEL_CODE'])
        thres_result['SCORE'] = thres_result['SCORE'].astype(float)
        thres_result['THRES'] = thres_result['THRES'].astype(float)
        thres_result = thres_result[thres_result['SCORE']<thres_result['THRES']].copy()

        if len(thres_result) != 0:
            p0 = f'''
            SELECT ORG_CODE,MODEL_CODE,SCORE FROM
            (select ORG_CODE,MODEL_CODE,SCORE,ROW_NUMBER() OVER ( PARTITION BY ORG_CODE,MODEL_CODE ORDER BY LASTUPDATE DESC ) AS RN
            from BD_RISK_CROSS_TRADE_SCORE 
            WHERE ISCURRENT = 1 
            AND MODEL_CODE IN ('TOTAL','SUPERVISOR','INNOVATION','CREDIT','LOGISTIC','THROUGH_CUSTOM','SUPPLY_CHAIN','STOCK','PUBLIC','PRODUCTION','FINANCE') 
            AND BUSINESS_TIME = trunc(add_months(sysdate,-2),'mm'))
            WHERE RN = 1 '''
            score_list_pre = Read_Oracle().read_oracle(sql=p0, database='dbalarm')

            if len(score_list_pre) == 0:
                print(f'Error: 未取到上个周期的分数数据.')
                score_list_pre = pd.DataFrame(columns = ['CORP_CREDIT_CODE','MODEL_CODE','PRE_SCORE'])
            else:
                score_list_pre.columns = ['CORP_CREDIT_CODE','MODEL_CODE','PRE_SCORE']

            alarm_result = pd.merge(thres_result,score_list_pre,how='left',on = ['CORP_CREDIT_CODE','MODEL_CODE'])
            alarm_result['PRE_SCORE'] = alarm_result['PRE_SCORE'].fillna(self.fillna_score)
            alarm_result.columns = ['BUSINESS_TYPE', 'CORP_CREDIT_CODE', 'SCORE', 'CORP_NAME', 'TRADE_CODE','THRES', 'PRE_SCORE']
            now = datetime.datetime.now()
            alarm_result['CUSTOMS_CODE'] = '2249'
            alarm_result['TYPE_FIRST'] = 'QYHXYJ'    
            alarm_result['ORDER_TYPE'] = 'trade'
            alarm_result['BUSINESS_NO'] = alarm_result['CORP_CREDIT_CODE']
            alarm_result['RESOLVE_STATUS'] = '0'
            alarm_result['RISK_LEVEL'] = '3'
            alarm_result['CREATE_TIME'] = now
            alarm_result['UPDATE_TIME'] = now
            alarm_result['CONTEXT'] = alarm_result.loc[:,['SCORE', 'PRE_SCORE','THRES']].apply(
                                    lambda x: json.dumps({'currentScore': str(x['SCORE']),'warningScore': str(x['THRES']),
                                                        'lastScore': str(x['PRE_SCORE'])},
                                                        ensure_ascii=False),axis = 1)
            alarm_result['CREATE_TIME'] = alarm_result['CREATE_TIME'].astype('datetime64')
            alarm_result['UPDATE_TIME'] = alarm_result['UPDATE_TIME'].astype('datetime64')

            alarm_single = alarm_result[alarm_result['BUSINESS_TYPE'] != 'TOTAL'].copy()
            alarm_total = alarm_result[alarm_result['BUSINESS_TYPE'] == 'TOTAL'].copy()
            if len(alarm_single) != 0:
                alarm_single['TYPE_SECOND'] = 'QYDXFYJ'
                alarm_single = alarm_single[['CUSTOMS_CODE','CORP_CREDIT_CODE','CORP_NAME','TRADE_CODE','TYPE_FIRST',
                                            'TYPE_SECOND','BUSINESS_TYPE','ORDER_TYPE','BUSINESS_NO','CONTEXT',
                                            'RESOLVE_STATUS','CREATE_TIME','UPDATE_TIME','RISK_LEVEL']]
                alarm_single = alarm_single.reset_index(drop=True)
                alarm_single = alarm_single.reset_index().rename(columns={'index': 'ID'})
                Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_WARAIN_TEMP', alarm_single, None,alarm=None)

            if len(alarm_total) != 0:
                alarm_total['TYPE_SECOND'] = 'QYZFYJ'
                alarm_total = alarm_total[['CUSTOMS_CODE','CORP_CREDIT_CODE','CORP_NAME','TRADE_CODE','TYPE_FIRST',
                                        'TYPE_SECOND','BUSINESS_TYPE','ORDER_TYPE','BUSINESS_NO','CONTEXT',
                                        'RESOLVE_STATUS','CREATE_TIME','UPDATE_TIME','RISK_LEVEL']]
                alarm_total = alarm_total.reset_index(drop=True)
                alarm_total = alarm_total.reset_index().rename(columns={'index': 'ID'})
                Write_Oracle_Alarm().write_oracle('BD_RISK_CROSS_TRADE_WARAIN_TEMP', alarm_total, None,alarm=None)
        else:
            print('无分数预警')

        
    def run_score_alarm(self):
        try:
            self.score_alarm()
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
    ScoreAlarm(child_task_id, org_code, params).run_score_alarm()