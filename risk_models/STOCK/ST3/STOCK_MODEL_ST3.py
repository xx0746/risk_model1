import sys, os
from os import path

sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_DETAIL_STOCK_ST3, _name_BD_RISK_RESULT_STOCK_ST3


class StockModelSt3(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'STOCK'
        self.child_model_code = 'ST3'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.score1 = json.loads(params)['score1']
        self.score2 = json.loads(params)['score2']
        self.thresh_hold_high = json.loads(params)['thresh_hold_high'] * 0.01
        self.thresh_hold_low = json.loads(params)['thresh_hold_low'] * 0.01
        self.turnover_rate = json.loads(params)['turnover_rate']

    def model_st3(self):
        # 读取sql
        sql = '''select * from {} where iscurrent =1 and ORG_CODE = '{}' and CUSTOMER_CODE = 'FTA_LG' '''.format(
            _name_BD_RISK_DETAIL_STOCK_ST3,
            self.org_code)
        BD_RISK_DETAIL_STOCK_ST3 = Read_Oracle().read_oracle(sql=sql, database='dbdm')
        # 合成时间字段
        BD_RISK_DETAIL_STOCK_ST3['BUSINESS_DATE'] = BD_RISK_DETAIL_STOCK_ST3['ACTRUAL_STOCK_DATE'].apply(
            lambda x: '{}年{}月'.format(x.year, x.month))
        # 拷贝dataframe
        df = BD_RISK_DETAIL_STOCK_ST3.copy()
        # 赋值年份
        df['YEAR'] = df.ACTRUAL_STOCK_DATE.dt.year
        measure_list = []
        # 取前30的货类
        for key, i in df.groupby('UNIT_CO')['QTY_CO'].sum().sort_values(ascending=False).head(30).items():
            measure_list.append(key)
        # 货类过滤
        df = df[df['UNIT_CO'].apply(lambda x: x in measure_list)]

        # # 公式 1、库存吞吐量 = 该期间的出库各计量单位数量+该期间的入库各计量单位数量
        df_take = df[(df['BILL_TYPE'] == '2')]

        df_back = df[(df['BILL_TYPE'] == '1')]

        df_back = df_back[['UNIT_CO', 'QTY_CO', 'BUSINESS_DATE']].groupby(['BUSINESS_DATE', 'UNIT_CO'])[
            'QTY_CO'].sum().reset_index()
        df_take = df_take[['UNIT_CO', 'QTY_CO', 'BUSINESS_DATE']].groupby(['BUSINESS_DATE', 'UNIT_CO'])[
            'QTY_CO'].sum().reset_index()
        df_measure = df_take.merge(df_back, on=['UNIT_CO', 'BUSINESS_DATE']).rename(
            columns={'QTY_CO_x': 'in_num', 'QTY_CO_y': 'out_num'})

        df_measure['VOLUMN'] = df_measure['in_num'] + df_measure['out_num']
        df_bench = df_measure.groupby(['UNIT_CO'])['VOLUMN'].quantile([self.thresh_hold_low, self.thresh_hold_high])
        df_bench = pd.DataFrame(df_bench).unstack()['VOLUMN']
        df_bench = df_bench.reset_index().rename(columns={self.thresh_hold_low: 'low', self.thresh_hold_high: 'high'})
        df_m1 = df_measure.merge(df_bench, on='UNIT_CO')
        df_m1['RISK_LABEL'] = (np.where(df_m1['VOLUMN'] < df_m1['low'], '吞吐量偏低',
                                        np.where(df_m1['VOLUMN'] > df_m1['high'], '吞吐量偏高',
                                                 '吞吐量正常')))
        # df_m1 = df_m1.drop(['in_num', 'out_num', 'low', 'high', 'UNIT_CO'], axis=1)
        df_m1 = df_m1.drop(['in_num', 'out_num', 'low', 'high'], axis=1)

        df = BD_RISK_DETAIL_STOCK_ST3.copy()
        # 记得删掉 !!!!!!!!!!!!!!!!!!! 模拟数据# TODO 金额目前都为0，数据质量问题
        # df['TRADE_TOTAL'] = np.random.randint(1000000, size=len(df))

        # 生成进库df 和 出库df
        df_take = df[(df['BILL_TYPE'] == '2')]
        df_back = df[(df['BILL_TYPE'] == '1')]
        df_back = df_back[['TRADE_TOTAL', 'BUSINESS_DATE', 'UNIT_CO']].groupby(['BUSINESS_DATE', 'UNIT_CO'])[
            'TRADE_TOTAL'].sum().reset_index()
        df_take = df_take[['TRADE_TOTAL', 'BUSINESS_DATE', 'UNIT_CO']].groupby(['BUSINESS_DATE', 'UNIT_CO'])[
            'TRADE_TOTAL'].sum().reset_index()
        # 汇总成总total money
        df_m2 = df_take.merge(df_back, on=['BUSINESS_DATE', 'UNIT_CO']).rename(
            columns={'TRADE_TOTAL_x': 'in_money', 'TRADE_TOTAL_y': 'out_money'})
        df_m2['total_money'] = df_m2['in_money'] + df_m2['out_money']
        # 库存周转率 = 该期间的出库总金额数量/该期间的进出库总金额
        df_m2['TRADE_RATE'] = df_m2['out_money'] / df_m2['total_money'].mean() * 100
        df_m2['RISK_LABEL'] = (np.where(df_m2
                                        ['TRADE_RATE'] > self.turnover_rate, '库存周转率异常', '库存周转率正常'))
        df_m2 = df_m2.drop(['in_money', 'out_money', 'total_money'], axis=1)
        df_final = df_m1.merge(df_m2, on=['BUSINESS_DATE', 'UNIT_CO'], how='left')
        df_final['ORG_CODE'] = self.org_code
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')

        df_final['SCORE'] = df_final['RISK_LABEL_x'].apply(lambda x: 0 if '正常' in x else self.score1) + df_final[
            'RISK_LABEL_y'].apply(lambda x: 0 if '正常' in x else self.score2)
        df_final['CHECK_TIME'] = now
        df_final['CHECK_TIME'] = pd.to_datetime(df_final['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')

        df_final['RISK_LABEL'] = df_final['RISK_LABEL_x'] + ',' + df_final['RISK_LABEL_y']

        df_final = df_final.drop(['RISK_LABEL_x', 'RISK_LABEL_y'], axis=1).reset_index().rename(columns={'index': 'ID'})

        # 明确企业所属租户
        df_final['CUSTOMER_CODE'] = 'FTA_LG'

        # 写入结果表
        Write_Oracle().write_oracle(_name_BD_RISK_RESULT_STOCK_ST3, df_final, org_code=self.org_code, alarm=None)
        # 更新预警表
        ttl_num = df_final['RISK_LABEL'].apply(lambda x: 1 if '正常' not in x.split(',')[0] else 0).sum()
        zzl_num = df_final['RISK_LABEL'].apply(lambda x: 1 if '正常' not in x.split(',')[1] else 0).sum()
        alarm_reason = ['发现{}起库存吞吐异常事件'.format(ttl_num), '发现{}起库存周转率异常事件'.format(zzl_num)]
        df_alarm = []
        for i in alarm_reason:
            temp = {}
            temp['CHECK_TIME'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            temp['ORG_CODE'] = self.org_code
            temp['MODEL_CODE'] = self.model_code
            temp['CHILD_MODEL_CODE'] = self.child_model_code
            temp['ALARM_REASON'] = i
            temp['ALARM_NUMBER'] = int(re.findall('发现(.*?)起.*', i)[0])
            df_alarm.append(temp)
        df_alarm = pd.DataFrame(df_alarm)
        df_alarm = df_alarm.reset_index().rename(columns={'index': 'ID'})
        df_alarm['CHECK_TIME'] = pd.to_datetime(df_alarm['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')

        # 明确企业所属租户
        df_alarm['CUSTOMER_CODE'] = 'FTA_LG'

        if df_alarm.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle('BD_RISK_ALARM_ITEM', df_alarm, org_code=self.org_code,
                                        alarm=[self.model_code, self.child_model_code])

    def run_model_st3(self):
        exec_status = None
        try:
            self.model_st3()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = 'a1d3471621f949fa8faec09a6253af0b'
    org_code, param_json, base_time = read_log_table(child_task_id)
    StockModelSt3(org_code=org_code, params=param_json, base_time=base_time,
                  child_task_id=child_task_id).run_model_st3()
