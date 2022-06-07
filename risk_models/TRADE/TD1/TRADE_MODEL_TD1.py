import sys, os
from os import path

sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_CUSTOMS_CREDIT, _name_BD_RISK_CORP_INFO_BASIC,_name_BD_RISK_RESULT_TRADE_TD1


class TradeModelTd1(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'TRADE'
        self.child_model_code = 'TD1'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.score = json.loads(params)['score']


    def model_td1(self):
        org_code=self.org_code
        score= self.score
        sql = f'''
               select title,case_date,corp_name ,cus_code,legal_rep,entry_num,
               case  when case_type = 'zswg' then '走私违规'
                     when case_type = 'jyjy' then '检验检疫'
                     when case_type = 'zscq' then '知识产权' 
               end as case_type
               from {_name_CUSTOMS_CREDIT} where iscurrent =1
               '''
        df_news = Read_Oracle().read_oracle(sql=sql, database='dbods')
        sql = f'''
        select ORG_CODE,org_name from {_name_BD_RISK_CORP_INFO_BASIC} where iscurrent =1 
        and ORG_CODE = '{org_code}'
        '''
        df_corp = Read_Oracle().read_oracle(sql=sql, database='dbods')
        df = df_news.merge(df_corp, left_on='CORP_NAME', right_on='ORG_NAME', how='inner')
        if len(df) == 0:
            logger.info(f'该{org_code}没有发现案件信息')
        else:
            df.drop(['CORP_NAME'], axis=1, inplace=True)
            df.rename(columns={'TITLE': 'CASE_TITLE'}, inplace=True)
            df['SCORE'] = score
            df.fillna('', inplace=True)
            df['CHECK_TIME'] = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
            df['CHECK_TIME'] = pd.to_datetime(df['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
            df = df.reset_index().rename(columns={'index': 'ID'})
            df['CASE_DATE'] = pd.to_datetime(df['CASE_DATE'], format='%Y-%m-%d')
            
            # 明确企业所属租户
            df['CUSTOMER_CODE'] = 'FTA_LG'
            
            Write_Oracle().write_oracle(_name_BD_RISK_RESULT_TRADE_TD1, df, org_code=org_code, alarm=None)

            def get_num(case_type):
                '''获取案件数量'''
                try:
                    return df[df['CASE_TYPE'] == case_type].groupby(['CASE_TYPE'])['ID'].count().iloc[0]
                except:
                    return 0

            num1 = get_num('走私违规')
            num2 = get_num('检验检疫')
            num3 = get_num('知识产权')
            alarm_reason = [{'case_type': '走私违规', 'num': num1},
                            {'case_type': '检验检疫', 'num': num2},
                            {'case_type': '知识产权', 'num': num3}]
            df_alarm = []
            for i in alarm_reason:
                if i['num'] != 0:
                    temp = {}
                    temp['CHECK_TIME'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    temp['ORG_CODE'] = org_code
                    temp['MODEL_CODE'] = self.model_code
                    temp['CHILD_MODEL_CODE'] = self.child_model_code
                    temp['ALARM_REASON'] = f'发现涉及' + i['case_type'] + '案件' + str(i['num']) + '起'
                    temp['ALARM_NUMBER'] = int(i['num'])
                    df_alarm.append(temp)
                else:
                    pass
            df_alarm = pd.DataFrame(df_alarm)
            df_alarm = df_alarm.reset_index().rename(columns={'index': 'ID'})
            df_alarm['CHECK_TIME'] = pd.to_datetime(df_alarm['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
            # 明确企业所属租户
            df_alarm['CUSTOMER_CODE'] = 'FTA_LG'
            if df_alarm.empty:
                logger.info('没有异常情况')
            else:
                Write_Oracle().write_oracle('BD_RISK_ALARM_ITEM', df_alarm, org_code=org_code,
                                            alarm=[self.model_code, self.child_model_code])


    def run_model_td1(self):
        exec_status = None
        try:
            self.model_td1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = 'childtaskidtd1001'
    # child_task_id = 'childtaskidtd1002'
    org_code, param_json, base_time = read_log_table(child_task_id)
    TradeModelTd1(org_code=org_code, params=param_json, base_time=base_time,
                  child_task_id=child_task_id).run_model_td1()
