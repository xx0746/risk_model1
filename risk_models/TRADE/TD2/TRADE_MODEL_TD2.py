import sys, os
from os import path

sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *

from risk_models import _name_FT_STA_GOODSOWNER_MAIN_CLASS, _name_DW_CORP_CUSDEC, _name_DIM_FTZ_CORP, \
    _name_FT_CUS_DWS_ENTRY, _name_DIM_TRADER


class TradeModelTd2(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'TRADE'
        self.child_model_code = 'TD2'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.return_score = json.loads(params)['return_score']
        self.hs_score = json.loads(params)['hs_score']
        self.ins_score = json.loads(params)['ins_score']
        self.nopass_score = json.loads(params)['nopass_score']
        dt = (dt_method.strptime(base_time, "%Y-%m-%d %H:%M:%S")).strftime('%Y-%m-%d')

        dt = dt_method.strptime(dt, "%Y-%m-%d")
        target_dt = datetime.datetime(dt.year, dt.month - 1, 1)
        self.target_dt = pd.DatetimeIndex([target_dt])[0]

    def model_td2(self):
        # 确定目标业务时间
        target_dt = self.target_dt
        org_code = self.org_code

        # 企业货值与货类统计sql
        sql1 = f'''
        select c.UNF_SC_ID, a.TRADE_CODE,a.START_TIME,a.End_time,a.TRADE_NAME_CN,a.I_E_MARK,
        a.CARGO_SUB_CATEGORY_CODE,a.CARGO_SUB_CATEGORY_NAME,a.SUM_GOODS_GROSS_VALUE_RMB
        from {_name_FT_STA_GOODSOWNER_MAIN_CLASS} a 
        left join {_name_DW_CORP_CUSDEC} c
        on a.trade_code = c.CUS_CODE_ENT
        inner join {_name_DIM_FTZ_CORP} b
        on c.UNF_SC_ID = b.UNI_SC_ID AND b.zmxpq_flag = 1 
        where c.isdeleted =0
        '''
        df_model1 = Read_Oracle().read_oracle(sql=sql1, database='dbdw')
        # 退单率查验率数据读取
        sql2 = f'''select to_char(to_date(a.STARTDT),'yyyy/mm')  as startdt,
        sum(SUM_ENTRY) as SUM_ENTRY,
        sum(SUM_RETURN_FLAG) as SUM_RETURN_FLAG,
        sum(SUM_INSPECTION) as SUM_INSPECTION,
        sum(SUM_INSPECTION_NOPASS) as SUM_INSPECTION_NOPASS,
        TRADE_CODE_SCC,TRADE_NAME_CN from {_name_FT_CUS_DWS_ENTRY} a 
        left join {_name_DIM_TRADER} c on a.TRADE_CODE_KEY = c.TRADE_KEY
        inner join {_name_DIM_FTZ_CORP} b on c.TRADE_CODE_SCC = b.UNI_SC_ID AND b.zmxpq_flag = 1 
        where c.iscurrent =1
        group by TRADE_CODE_SCC,TRADE_NAME_CN, to_char(to_date(a.STARTDT),'yyyy/mm')'''
        df_model2 = Read_Oracle().read_oracle(sql=sql2, database='dbdw')
        df = df_model1.groupby(['UNF_SC_ID', 'START_TIME'])['SUM_GOODS_GROSS_VALUE_RMB'].sum().reset_index()

        # 构建基础df
        df = df[(df['START_TIME'] == target_dt)]
        # 构建目标企业的df
        final = df[(df['START_TIME'] == target_dt) & (df['UNF_SC_ID'] == org_code)]
        # 挖掘企业陌生货类，先构建历史的交易货类
        old_list = \
            df_model1[df_model1['START_TIME'].apply(lambda x: x != target_dt) & (df_model1['UNF_SC_ID'] == org_code)][
                ['CARGO_SUB_CATEGORY_CODE', 'CARGO_SUB_CATEGORY_NAME']].to_dict('records')
        # 构建当月的交易货类清单
        new_list = \
            df_model1[df_model1['START_TIME'].apply(lambda x: x == target_dt) & (df_model1['UNF_SC_ID'] == org_code)][
                ['CARGO_SUB_CATEGORY_CODE', 'CARGO_SUB_CATEGORY_NAME']].to_dict('records')
        # 取交集并去重
        error_list = []
        for new_type in new_list:
            if new_type not in old_list:
                error_list.append(new_type)
        # 列表去重，构建新的企业货类列表
        new_l1 = []
        for i in error_list:
            if i not in new_l1:
                new_l1.append(i)
        s = final.copy()
        s.loc[:, 'NEW_HSCODE_LIST'] = json.dumps(new_l1, ensure_ascii=False)
        final = s #这么写诗为了避免warning
        # 转化时间格式
        df_model2['STARTDT'] = df_model2['STARTDT'].apply(lambda x: datetime.datetime.strptime(x, '%Y/%m'))
        df_model2['RETURN_RATE'] = df_model2['SUM_INSPECTION_NOPASS'] / df_model2['SUM_ENTRY']
        df_model2['RETURN_RATE'] = df_model2['RETURN_RATE'].apply(lambda x: round(x, 3))
        df_model2['INSPECTION_RATE'] = df_model2['SUM_INSPECTION'] / df_model2['SUM_ENTRY']
        df_model2['INSPECTION_RATE'] = df_model2['INSPECTION_RATE'].apply(lambda x: round(x, 3))
        df2 = df_model2[
            ['STARTDT', 'SUM_ENTRY', 'RETURN_RATE', 'INSPECTION_RATE', 'SUM_INSPECTION_NOPASS', 'TRADE_CODE_SCC',
             'TRADE_NAME_CN']]
        final = final.merge(df2, left_on=['UNF_SC_ID', 'START_TIME'], right_on=['TRADE_CODE_SCC', 'STARTDT'],
                            how='left')
        final = final.drop(columns=['STARTDT', 'TRADE_CODE_SCC'], axis=1)
        final.rename(
            columns={'UNF_SC_ID': 'ORG_CODE', 'START_TIME': 'BIZ_DATE', 'SUM_GOODS_GROSS_VALUE_RMB': 'ENTRY_VALUE',
                     'SUM_ENTRY': 'ENTRY_NUM', 'SUM_INSPECTION_NOPASS': 'NOPASS_NUM', 'TRADE_NAME_CN': 'ORG_NAME'},
            inplace=True)
        # 开始赋予异常标签和分数 查看陌生货类的列表长度
        risk_a = final['NEW_HSCODE_LIST'].apply(
            lambda x: '发现{}起陌生交易货类异常'.format(len(json.loads(x))) if len(json.loads(x)) > 0 else '')
        # 计算退单率，查验率和查获事件
        #TODO 这里需要改一下每个标签的类型
        risk_b = final['RETURN_RATE'].apply(lambda x: '退单率异常' if x >= 0.1 else '退单率正常')
        risk_c = final['INSPECTION_RATE'].apply(lambda x: '查验率异常' if x >= 0.1 else '查验率正常')
        risk_d = final['NOPASS_NUM'].apply(lambda x: '发现{}起海关查获事件'.format(x) if x > 0 else '')
        # 组合异常标签
        final['RISK_LABEL'] = ';'.join(i for i in [risk_a[0], risk_b[0], risk_c[0], risk_d[0]] if i != '')
        # 计算分数
        score_a = final['NEW_HSCODE_LIST'].apply(lambda x: self.hs_score * len(json.loads(x)) if len(json.loads(x)) > 0 else 0)
        score_b = final['RETURN_RATE'].apply(lambda x: self.return_score * ((x - 0.1) / 0.1) if x >= 0.1 else 0)
        score_c = final['INSPECTION_RATE'].apply(lambda x: self.ins_score * ((x - 0.1) / 0.1) if x >= 0.1 else 0)
        score_d = final['NOPASS_NUM'].apply(lambda x:  self.nopass_score * x if x > 0 else 0)
        final['SCORE'] = score_a + score_b + score_c + score_d
        final['CHECK_TIME'] = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        final['CHECK_TIME'] = pd.to_datetime(final['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        final = final.reset_index().rename(columns={'index': 'ID'})
        final['CUSTOMER_CODE'] = 'FTA_LG'
        Write_Oracle().write_oracle('BD_RISK_RESULT_TRADE_TD2', final, org_code=org_code, alarm=None, iscurrent=False)

        alarm_reason = [i for i in [risk_a[0], risk_b[0], risk_c[0], risk_d[0]] if i != '']
        df_alarm = []
        def get_nopass_event():
            if re.findall('发现(.*?)起.*?', i) == []:
                return 0
            else:
                return int(re.findall('发现(.*?)起.*?', i)[0])
        for i in alarm_reason:
            temp = {}
            temp['CHECK_TIME'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            temp['ORG_CODE'] = org_code
            temp['MODEL_CODE'] = self.model_code
            temp['CHILD_MODEL_CODE'] = self.child_model_code
            temp['ALARM_REASON'] = i
            temp['ALARM_NUMBER'] = get_nopass_event() + len(re.findall('退单率异常', i)) + len(
                re.findall('查验率异常', i))
            df_alarm.append(temp)
        df_alarm = pd.DataFrame(df_alarm)
        df_alarm = df_alarm.reset_index().rename(columns={'index': 'ID'})
        df_alarm['CHECK_TIME'] = pd.to_datetime(df_alarm['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        df_alarm['CUSTOMER_CODE'] = 'FTA_LG'
        if df_alarm.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle('BD_RISK_ALARM_ITEM', df_alarm, org_code=self.org_code,
                                        alarm=[self.model_code, self.child_model_code])

    def run_model_td2(self):
        exec_status = None
        try:
            self.model_td2()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = 'childtaskidtd2002'
    org_code, param_json, base_time = read_log_table(child_task_id)
    TradeModelTd2(org_code=org_code, params=param_json, base_time=base_time,
                  child_task_id=child_task_id).run_model_td2()
