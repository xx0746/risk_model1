import sys, os
from os import path

sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class StockModelSt5(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'STOCK'
        self.child_model_code = 'ST5'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        # 默认为30
        js_para = json.loads(params)
        if 'days_thr' in js_para:
            self.days_thr = js_para['days_thr']
        # 默认为1
        if 'rate_thr_sup' in js_para:
            self.rate_thr_sup = js_para['rate_thr_sup']
        if 'rate_thr_inf' in js_para:
            self.rate_thr_inf = js_para['rate_thr_inf']
        # 默认为360
        if 'day_thr_sup' in js_para:
            self.day_thr_sup = js_para['day_thr_sup']
        # 默认为None
        if 'day_per_thr_sup' in js_para:
            self.day_per_thr_sup = js_para['day_per_thr_sup']
        else:
            self.day_per_thr_sup = None
        # 默认为 [0,-0.5,-0.1,-0.5]
        if 'score_crit' in js_para:
            self.score_crit = js_para['score_crit']

    def model_st5(self):

        # 应加入公司ID筛选，默认取ISCURRENT为1的数据，
        sql = f"""
        select * from {TableList.BD_RISK_DETAIL_STOCK_ST5.value} WHERE ISCURRENT = 1 AND (DAY_DIFF > {self.days_thr} OR QTY_DELIV IS NOT NULL) AND ORG_CODE LIKE '{self.org_code}' and CUSTOMER_CODE = 'FTA_LG' \
        """
        MODEL_DETAIL = Read_Oracle().read_oracle(sql=sql, database='dbdm')

        MODEL_DETAIL = MODEL_DETAIL.drop(columns=['ISCURRENT', 'LASTUPDATE'])
        data_length = MODEL_DETAIL.shape[0]

        # 分开处理
        df_not_deliv = MODEL_DETAIL[MODEL_DETAIL['DELIV_FLAG'] == 0].copy()
        df_deliv = MODEL_DETAIL[MODEL_DETAIL['DELIV_FLAG'] == 1].copy()

        # 对未交付的打上未交付的标签
        # 根据平均每百条数据的未交付情况扣分，每百条未交付一条的扣0.5分
        df_not_deliv['RISK_LABEL'] = '未交付'
        df_not_deliv['SCORE'] = (self.score_crit[3] / data_length * 100)

        if pd.isnull(self.day_per_thr_sup):
            day_per_thr_sup = df_deliv.loc[df_deliv['RATE'] < self.rate_thr_inf, 'DAY_DIFF'].quantile(0.9)
        else:
            day_per_thr_sup = self.day_per_thr_sup
        # 若阈值为空，则设为RATE的90分位数与10分位数

        df_deliv['RISK_LABEL'] = '数据正常'
        df_deliv['SCORE'] = self.score_crit[0]
        deliv_count = df_deliv.shape[0]

        # 根据平均每百条数据的交付情况扣分，每百条交付数量缺少且满足条件的扣0.1分
        df_deliv.loc[(df_deliv['RATE'] < self.rate_thr_inf) & (df_deliv['DAY_DIFF'] > day_per_thr_sup) & (
                    df_deliv['DAY_DIFF'] > self.day_thr_sup), 'RISK_LABEL'] = '入库数量过少'
        df_deliv.loc[(df_deliv['RATE'] < self.rate_thr_inf) & (df_deliv['DAY_DIFF'] > day_per_thr_sup) & (
                    df_deliv['DAY_DIFF'] > self.day_thr_sup), 'SCORE'] = (self.score_crit[2] / deliv_count * 100)

        # 根据平均每百条数据的交付情况扣分，每百条交付数量过多且满足条件的扣0.5分
        df_deliv.loc[df_deliv['RATE'] > self.rate_thr_sup, 'RISK_LABEL'] = '入库数量过多'
        df_deliv.loc[df_deliv['RATE'] > self.rate_thr_sup, 'SCORE'] = (self.score_crit[1] / deliv_count * 100)

        # 合并
        df_result = pd.concat([df_not_deliv, df_deliv])
        #         # 剔除非负分的数据（即只保留异常数据）
        #         df_result = df_result[df_result['SCORE']<0].copy()

        # 只保留需要的字段
        df_result = df_result[
            ['ORG_CODE', 'ORDER_NO', 'COP_G_NO', 'SIGN_DT', 'QTY_ORDER', 'QTY_DELIV', 'QTY_STOCK', 'RISK_LABEL',
             'SCORE']].copy()

        # 添加CHECK_TIME
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df_result['CHECK_TIME'] = now
        df_result['CHECK_TIME'] = pd.to_datetime(df_result['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')

        # 格式确认，重设INDEX后添加ID列
        df_result['ORG_CODE'] = df_result['ORG_CODE'].astype(str)
        df_result['ORDER_NO'] = df_result['ORDER_NO'].astype(str)
        df_result['COP_G_NO'] = df_result['COP_G_NO'].astype(str)
        df_result = df_result.reset_index(drop=True)
        df_result = df_result.reset_index().rename(columns={'index': 'ID'})
        df_result['CHECK_TIME'] = df_result['CHECK_TIME'].astype('datetime64')
        df_result['SIGN_DT'] = df_result['SIGN_DT'].astype('datetime64')
        df_result['QTY_ORDER'] = df_result['QTY_ORDER'].astype(float)
        df_result['QTY_STOCK'] = df_result['QTY_STOCK'].astype(float)
        df_result['QTY_DELIV'] = df_result['QTY_DELIV'].astype(float)
        df_result['SCORE'] = df_result['SCORE'].astype(float)
        df_result['CUSTOMER_CODE'] = 'FTA_LG'

        # print(df_result['SCORE'].sum())

        # 写入
        Write_Oracle().write_oracle(f'{TableList.BD_RISK_RESULT_STOCK_ST5.value}', df_result, org_code=self.org_code,
                                    alarm=None)

        # 整理预警明细数据，并写入数据库
        RISK_ALARM = df_result[df_result['SCORE'] != 0].groupby(['RISK_LABEL'], as_index=False)['ID'].count()
        RISK_ALARM = RISK_ALARM.rename(columns={'ID': 'ALARM_NUMBER'})
        RISK_ALARM['ALARM_REASON'] = '发现' + RISK_ALARM['ALARM_NUMBER'].astype('str') + '起' + RISK_ALARM[
            'RISK_LABEL'] + '事件'
        RISK_ALARM['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        RISK_ALARM['ORG_CODE'] = self.org_code
        RISK_ALARM['MODEL_CODE'] = 'STOCK'
        RISK_ALARM['CHILD_MODEL_CODE'] = 'ST5'
        RISK_ALARM['ID'] = range(len(RISK_ALARM))
        RISK_ALARM = RISK_ALARM[
            ['ID', 'ORG_CODE', 'MODEL_CODE', 'CHILD_MODEL_CODE', 'ALARM_REASON', 'ALARM_NUMBER', 'CHECK_TIME']].copy()
        RISK_ALARM['CUSTOMER_CODE'] = 'FTA_LG'

        if RISK_ALARM.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle(f'{TableList.BD_RISK_ALARM_ITEM.value}', RISK_ALARM, org_code=self.org_code,
                                        alarm=['STOCK', 'ST5'])

    def run_model_st5(self):
        try:
            self.model_st5()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = 'bf274911eb9742639aeb3266fa006c38'
    org_code, param_json, basetime = read_log_table(child_task_id)
    StockModelSt5(org_code, params=param_json, base_time=basetime, child_task_id=child_task_id).run_model_st5()