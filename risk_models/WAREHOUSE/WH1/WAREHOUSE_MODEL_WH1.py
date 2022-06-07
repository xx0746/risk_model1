import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class WarehouseModelWh1(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'WAREHOUSE'
        self.child_model_code = 'WH1'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        # [正常，匹配不上报关单，匹配不上货物，贸易金额不符，法定数量不符，申报数量1不符，申报数量2不符，三种数量无法对应]
        self.score_crit = json.loads(params)['score_crit']


    def model_wh1(self):

        def thr_sup(num_x):
            if pd.isnull(num_x):
                return 0
            else:
                return (1+0.05)*num_x
            
        def thr_inf(num_x):
            if pd.isnull(num_x):
                return 0
            else:
                return (1-0.05)*num_x

        sql = f"""\
        SELECT * FROM {TableList.BD_RISK_DETAIL_WAREHOUSE_WH1_C.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code} and CUSTOMER_CODE = 'FTA_LG''
        """
        CUS_DATA = Read_Oracle().read_oracle(sql=sql, database='dbdm')

        sql = f"""\
        SELECT * FROM {TableList.BD_RISK_DETAIL_WAREHOUSE_WH1_S.value} WHERE ISCURRENT = 1 AND ORG_CODE LIKE '{self.org_code} and CUSTOMER_CODE = 'FTA_LG''
        """
        STOCK_DATA = Read_Oracle().read_oracle(sql=sql, database='dbdm')

        entry_list = CUS_DATA[['ENTRY_NO','GOODS_NO']].copy()
        entry_list = entry_list.drop_duplicates()
        entry_list['conn_flag'] = 1

        entry_only = CUS_DATA[['ENTRY_NO']].copy()
        entry_only = entry_only.drop_duplicates()
        entry_only['conn_flag_e'] = 1

        df = pd.merge(STOCK_DATA,entry_list,how = 'left',left_on=['ENTRY_NO','ENTRY_GDS_SEQNO'],right_on=['ENTRY_NO','GOODS_NO'])
        df = pd.merge(df,entry_only,how = 'left',left_on=['ENTRY_NO'],right_on=['ENTRY_NO'])

        df_match = df[-pd.isnull(df['conn_flag_e'])].copy()
        df_result = df[pd.isnull(df['conn_flag_e'])].copy()
        df_result = df_result.drop(columns = ['ID','ISCURRENT','CHECK_TIME','conn_flag_e','conn_flag','LASTUPDATE'])
        df_match = df_match.drop(columns = ['ID','ISCURRENT','CHECK_TIME','conn_flag_e','LASTUPDATE'])
        df_result['RISK_LABEL'] = '匹配不到相应报关单'
        df_result['SCORE'] = self.score_crit[1]

        x = df_match[pd.isnull(df_match['conn_flag'])].copy()
        x = x.drop(columns=['conn_flag'])
        x['RISK_LABEL'] = '匹配不到相应货物'
        x['SCORE'] = self.score_crit[2]
        df_result = pd.concat([df_result,x])
        df_match = df_match[-pd.isnull(df_match['conn_flag'])].copy()
        df_match = df_match.drop(columns=['conn_flag'])

        if df_match.shape[0]!=0:
            CHECK_DATA = CUS_DATA[['ENTRY_NO','GOODS_NO','DECL_TOTAL']].copy()
            CHECK_DATA['trade_thr_sup'] = CHECK_DATA['DECL_TOTAL'].apply(thr_sup)
            CHECK_DATA['trade_thr_inf'] = CHECK_DATA['DECL_TOTAL'].apply(thr_inf)
            CHECK_DATA = CHECK_DATA.drop(columns=['DECL_TOTAL'])
            df_match = pd.merge(df_match,CHECK_DATA,how='inner',left_on=['ENTRY_NO','GOODS_NO'],right_on=['ENTRY_NO','GOODS_NO'])
            x = df_match[(df_match['TRADE_TOTAL']>df_match['trade_thr_sup'])|(df_match['TRADE_TOTAL']<df_match['trade_thr_inf'])].copy()
            match_count = df_match.shape[0]
            x = x.drop(columns=['trade_thr_sup','trade_thr_inf'])
            x['RISK_LABEL'] = '贸易金额异常'
            x['SCORE'] = self.score_crit[3]/match_count*100
            df_match = df_match[(df_match['TRADE_TOTAL']<=df_match['trade_thr_sup'])&(df_match['TRADE_TOTAL']>=df_match['trade_thr_inf'])].copy()
            df_match = df_match.drop(columns=['trade_thr_sup','trade_thr_inf'])
            df_result = pd.concat([df_result,x])

        if df_match.shape[0]!=0:
            CHECK_DATA = CUS_DATA[['ENTRY_NO','GOODS_NO','G_UNIT','UNIT_1','UNIT_2','G_QTY','QTY_1','QTY_2']].copy()
            CHECK_DATA['qty_thr_sup_0'] = CHECK_DATA['G_QTY'].apply(thr_sup)
            CHECK_DATA['qty_thr_inf_0'] = CHECK_DATA['G_QTY'].apply(thr_inf)
            CHECK_DATA['qty_thr_sup_1'] = CHECK_DATA['QTY_1'].apply(thr_sup)
            CHECK_DATA['qty_thr_inf_1'] = CHECK_DATA['QTY_1'].apply(thr_inf)
            CHECK_DATA['qty_thr_sup_2'] = CHECK_DATA['QTY_2'].apply(thr_sup)
            CHECK_DATA['qty_thr_inf_2'] = CHECK_DATA['QTY_2'].apply(thr_inf)
            CHECK_DATA = CHECK_DATA.drop(columns=['G_QTY','QTY_1','QTY_2'])
            CHECK_DATA.columns = ['ENTRY_NO','GOODS_NO','UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2']
            CHECK_DATA['UNIT_CHECK_0'] = CHECK_DATA['UNIT_CHECK_0'].astype(str)
            CHECK_DATA['UNIT_CHECK_1'] = CHECK_DATA['UNIT_CHECK_1'].astype(str)
            CHECK_DATA['UNIT_CHECK_2'] = CHECK_DATA['UNIT_CHECK_2'].astype(str)

            df_match = pd.merge(df_match,CHECK_DATA,how='inner',left_on=['ENTRY_NO','GOODS_NO'],right_on=['ENTRY_NO','GOODS_NO'])

            x = df_match[df_match['G_UNIT']==df_match['UNIT_CHECK_0']].copy()
            df_match = df_match[df_match['G_UNIT']!=df_match['UNIT_CHECK_0']].copy()

            y = x[(x['QTY']>x['qty_thr_sup_0'])|(x['QTY']<x['qty_thr_inf_0'])].copy()
            x = x[(x['QTY']<=x['qty_thr_sup_0'])&(x['QTY']>=x['qty_thr_inf_0'])].copy()
            y = y.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            y['RISK_LABEL'] = '申报数量异常'
            y['SCORE'] = self.score_crit[4]/match_count*100
            x = x.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            x['RISK_LABEL'] = '数据正常'
            x['SCORE'] = self.score_crit[0]
            df_result = pd.concat([df_result,x,y])

        if df_match.shape[0]!=0:
            x = df_match[df_match['G_UNIT']==df_match['UNIT_CHECK_1']].copy()
            df_match = df_match[df_match['G_UNIT']!=df_match['UNIT_CHECK_1']].copy()

            y = x[(x['QTY']>x['qty_thr_sup_1'])|(x['QTY']<x['qty_thr_inf_1'])].copy()
            x = x[(x['QTY']<=x['qty_thr_sup_1'])&(x['QTY']>=x['qty_thr_inf_1'])].copy()
            y = y.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            y['RISK_LABEL'] = '申报数量异常'
            y['SCORE'] = self.score_crit[4]/match_count*100
            x = x.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            x['RISK_LABEL'] = '数据正常'
            x['SCORE'] = self.score_crit[0]
            df_result = pd.concat([df_result,x,y])

        if df_match.shape[0]!=0:
            x = df_match[df_match['G_UNIT']==df_match['UNIT_CHECK_2']].copy()
            df_match = df_match[df_match['G_UNIT']!=df_match['UNIT_CHECK_2']].copy()

            y = x[(x['QTY']>x['qty_thr_sup_2'])|(x['QTY']<x['qty_thr_inf_2'])].copy()
            x = x[(x['QTY']<=x['qty_thr_sup_2'])&(x['QTY']>=x['qty_thr_inf_2'])].copy()
            y = y.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            y['RISK_LABEL'] = '申报数量异常'
            y['SCORE'] = self.score_crit[4]/match_count*100
            x = x.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            x['RISK_LABEL'] = '数据正常'
            x['SCORE'] = self.score_crit[0]
            df_result = pd.concat([df_result,x,y])

        if df_match.shape[0]!=0:
            x = df_match[df_match['UNIT_1']==df_match['UNIT_CHECK_0']].copy()
            df_match = df_match[df_match['UNIT_1']!=df_match['UNIT_CHECK_0']].copy()

            y = x[(x['QTY_1']>x['qty_thr_sup_0'])|(x['QTY_1']<x['qty_thr_inf_0'])].copy()
            x = x[(x['QTY_1']<=x['qty_thr_sup_0'])&(x['QTY_1']>=x['qty_thr_inf_0'])].copy()
            y = y.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            y['RISK_LABEL'] = '申报数量匹配不上，法定数量1异常'
            y['SCORE'] = self.score_crit[5]/match_count*100
            x = x.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            x['RISK_LABEL'] = '数据正常'
            x['SCORE'] = self.score_crit[0]
            df_result = pd.concat([df_result,x,y])

        if df_match.shape[0]!=0:
            x = df_match[df_match['UNIT_1']==df_match['UNIT_CHECK_1']].copy()
            df_match = df_match[df_match['UNIT_1']!=df_match['UNIT_CHECK_1']].copy()

            y = x[(x['QTY_1']>x['qty_thr_sup_1'])|(x['QTY_1']<x['qty_thr_inf_1'])].copy()
            x = x[(x['QTY_1']<=x['qty_thr_sup_1'])&(x['QTY_1']>=x['qty_thr_inf_1'])].copy()
            y = y.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            y['RISK_LABEL'] = '申报数量匹配不上，法定数量1异常'
            y['SCORE'] = self.score_crit[5]/match_count*100
            x = x.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            x['RISK_LABEL'] = '数据正常'
            x['SCORE'] = self.score_crit[0]
            df_result = pd.concat([df_result,x,y])

        if df_match.shape[0]!=0:
            x = df_match[df_match['UNIT_1']==df_match['UNIT_CHECK_2']].copy()
            df_match = df_match[df_match['UNIT_1']!=df_match['UNIT_CHECK_2']].copy()

            y = x[(x['QTY_1']>x['qty_thr_sup_2'])|(x['QTY_1']<x['qty_thr_inf_2'])].copy()
            x = x[(x['QTY_1']<=x['qty_thr_sup_2'])&(x['QTY_1']>=x['qty_thr_inf_2'])].copy()
            y = y.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            y['RISK_LABEL'] = '申报数量匹配不上，法定数量1异常'
            y['SCORE'] = self.score_crit[5]/match_count*100
            x = x.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            x['RISK_LABEL'] = '数据正常'
            x['SCORE'] = self.score_crit[0]
            df_result = pd.concat([df_result,x,y])

        if df_match.shape[0]!=0:
            x = df_match[df_match['UNIT_2']==df_match['UNIT_CHECK_0']].copy()
            df_match = df_match[df_match['UNIT_2']!=df_match['UNIT_CHECK_0']].copy()

            y = x[(x['QTY_2']>x['qty_thr_sup_0'])|(x['QTY_2']<x['qty_thr_inf_0'])].copy()
            x = x[(x['QTY_2']<=x['qty_thr_sup_0'])&(x['QTY_2']>=x['qty_thr_inf_0'])].copy()
            y = y.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            y['RISK_LABEL'] = '仅法定数量2可匹配，法定数量2异常'
            y['SCORE'] = self.score_crit[6]/match_count*100
            x = x.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            x['RISK_LABEL'] = '数据正常'
            x['SCORE'] = self.score_crit[0]
            df_result = pd.concat([df_result,x,y])

        if df_match.shape[0]!=0:
            x = df_match[df_match['UNIT_2']==df_match['UNIT_CHECK_1']].copy()
            df_match = df_match[df_match['UNIT_2']!=df_match['UNIT_CHECK_1']].copy()

            y = x[(x['QTY_2']>x['qty_thr_sup_1'])|(x['QTY_2']<x['qty_thr_inf_1'])].copy()
            x = x[(x['QTY_2']<=x['qty_thr_sup_1'])&(x['QTY_2']>=x['qty_thr_inf_1'])].copy()
            y = y.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            y['RISK_LABEL'] = '仅法定数量2可匹配，法定数量2异常'
            y['SCORE'] = self.score_crit[6]/match_count*100
            x = x.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            x['RISK_LABEL'] = '数据正常'
            x['SCORE'] = self.score_crit[0]
            df_result = pd.concat([df_result,x,y])

        if df_match.shape[0]!=0:
            x = df_match[df_match['UNIT_2']==df_match['UNIT_CHECK_2']].copy()
            z = df_match[df_match['UNIT_2']!=df_match['UNIT_CHECK_2']].copy()

            y = x[(x['QTY_2']>x['qty_thr_sup_2'])|(x['QTY_2']<x['qty_thr_inf_2'])].copy()
            x = x[(x['QTY_2']<=x['qty_thr_sup_2'])&(x['QTY_2']>=x['qty_thr_inf_2'])].copy()
            y = y.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            y['RISK_LABEL'] = '仅法定数量2可匹配，法定数量2异常'
            y['SCORE'] = self.score_crit[6]/match_count*100
            x = x.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            x['RISK_LABEL'] = '数据正常'
            x['SCORE'] = self.score_crit[0]
            z = z.drop(columns=['UNIT_CHECK_0','UNIT_CHECK_1','UNIT_CHECK_2','qty_thr_sup_0','qty_thr_inf_0','qty_thr_sup_1','qty_thr_inf_1','qty_thr_sup_2','qty_thr_inf_2'])
            z['RISK_LABEL'] = '申报数量、法定数量1与法定数量2均匹配失败'
            z['SCORE'] = self.score_crit[7]/match_count*100
            df_result = pd.concat([df_result,x,y,z])

        df_result = df_result.drop(columns=['GOODS_NO'])

        df_result = df_result.reset_index().rename(columns = {'index':'ID'})
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result['CHECK_TIME']=now
        df_result['CHECK_TIME']= pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')

        df_result['SCORE'] = df_result['SCORE'].astype(float)
        df_result['CUSTOMER_CODE'] = 'FTA_LG'
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_RESULT_WAREHOUSE_WH1.value}', df_result,org_code=self.org_code,alarm=None)
        
        # 整理预警明细数据，并写入数据库
        RISK_ALARM = df_result[df_result['SCORE'] != 0].groupby(['RISK_LABEL'], as_index=False)['ID'].count()
        RISK_ALARM = RISK_ALARM.rename(columns={'ID':'ALARM_NUMBER'})
        RISK_ALARM['ALARM_REASON'] = '发现' + RISK_ALARM['ALARM_NUMBER'].astype('str') + '起' + RISK_ALARM['RISK_LABEL'] + '事件'
        RISK_ALARM['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        RISK_ALARM['ORG_CODE'] = self.org_code
        RISK_ALARM['MODEL_CODE'] = 'WAREHOUSE'
        RISK_ALARM['CHILD_MODEL_CODE'] = 'WH1'
        RISK_ALARM['ID'] = range(len(RISK_ALARM))
        RISK_ALARM = RISK_ALARM[['ID','ORG_CODE','MODEL_CODE','CHILD_MODEL_CODE','ALARM_REASON','ALARM_NUMBER','CHECK_TIME']].copy()
        RISK_ALARM['CUSTOMER_CODE'] = 'FTA_LG'
        
        if RISK_ALARM.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle(f'{TableList.BD_RISK_ALARM_ITEM.value}',RISK_ALARM, org_code = self.org_code, alarm = ['WAREHOUSE','WH1'])


    def run_model_wh1(self):
        try:
            self.model_wh1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉d
    child_task_id = sys.argv[1]
    # child_task_id = 'child_task_wh_1_2'
    org_code, param_json, base_time = read_log_table(child_task_id)
#     org_code = '91310115688774070B'
    WarehouseModelWh1(org_code, params=param_json, base_time = base_time, child_task_id=child_task_id).run_model_wh1()