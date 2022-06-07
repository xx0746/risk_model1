import sys, os
from os import path

sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_DETAIL_SUPPLYCHAIN_SC1, _name_BD_RISK_RESULT_SUPPLYCHAIN_SC1


class SupplyChainSc1(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'SUPPLYCHAIN'
        self.child_model_code = 'SC1'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.init_score = 100
        # params = '{"tt_score":[-5,-2,3,5],"tv_score":[-5,-2,3,5],"sf_score":[-3,-2,1,5],"in_score":[-2.5,-2,1,5], \
        #          "sin_score":[-2,-1,1,2.5], "rating_list" : [10,9,8,7,6,5,0] , "bvd_list":[10,8,5,0,-1,0] }'
        tt_list = json.loads(params)['tt_score']
        tv_list = json.loads(params)['tv_score']
        sf_list = json.loads(params)['sf_score']
        in_list = json.loads(params)['in_score']
        sin_list = json.loads(params)['sin_score']
        rating_list = json.loads(params)['rating_list']
        bvd_list = json.loads(params)['bvd_list']
        self.trade_times_score = {'[1, 2)': tt_list[0], '[2, 10)': tt_list[1], '[10, 50)': tt_list[2],
                                  '[50, 999999999999)': tt_list[3],'[0,1)':0}
        self.trade_volumn_score = {'[1, 100000)': tv_list[0], '[100000, 1000000)': tv_list[1],
                                   '[1000000, 10000000)': tv_list[2],
                                   '[10000000, 999999999999)': tv_list[3],'[0,1)':0}
        self.staff_score = {'[1, 100)': sf_list[0], '[100, 500)': sf_list[1], '[500, 1000)': sf_list[2],
                            '[1000, 999999999999)': sf_list[3],'[0,1)':0}
        self.income_score = {'[1, 100000)': in_list[0], '[100000, 1000000)': in_list[1],
                             '[1000000, 10000000)': in_list[2],
                             '[10000000, 999999999999)': in_list[3],'[0,1)':0}
        self.s_income_score = {'[1, 100000)': sin_list[0], '[100000, 1000000)': sin_list[1],
                               '[1000000, 10000000)': sin_list[2],
                               '[10000000, 999999999999)': sin_list[3],'[0,1)':0}
        self.rating_score = {'AAA': rating_list[0], 'AA': rating_list[1], 'A': rating_list[2],
                             'BBB': rating_list[3], 'BB': rating_list[4], 'B': rating_list[5], '0': rating_list[6]}
        self.bvd_rate = {'1.0': bvd_list[0], '2.0': bvd_list[1], '3.0': bvd_list[2], '4.0': bvd_list[3],
                         '5.0': bvd_list[4], '0.0': bvd_list[5]}
        # 后期尝试加入k-means进行聚类，目前先人工打标签
        self.trade_times_bin = [0,1, 3, 10, 50, 999999999999]
        self.trade_volumn_bin = [0,1, 100000, 1000000, 10000000, 999999999999]
        self.staff_bin = [0,1, 100, 500, 1000, 999999999999]
        self.income_bin = [0,1, 100000, 1000000, 10000000, 999999999999]
        self.s_income_bin = [0,1, 100000, 1000000, 10000000, 999999999999]

    def model_sc1(self):
        # 读取sql
        sql = '''select * from {} where iscurrent =1 and CUSTOMER_CODE = 'FTA_LG' '''.format(_name_BD_RISK_DETAIL_SUPPLYCHAIN_SC1)
        df_sc = Read_Oracle().read_oracle(sql=sql, database='dbdm')

        df_sc['RATING'] = df_sc['RATING'].map(str)
        df_sc['RATING'] = df_sc['RATING'].map(self.rating_score)

        df_sc['BVD_RATE'] = df_sc['BVD_RATE'].map(str)
        df_sc['BVD_RATE'] = df_sc['BVD_RATE'].map(self.bvd_rate)

        df_sc['TRADE_TIMES'] = df_sc['TRADE_TIMES'].fillna(0)
        df_sc['TRADE_TIMES'] = df_sc['TRADE_TIMES'].map(float)
        df_sc['TRADE_TIMES'] = pd.cut(df_sc['TRADE_TIMES'], bins=self.trade_times_bin, right=False)
        df_sc['TRADE_TIMES'] = df_sc['TRADE_TIMES'].map(str).map(self.trade_times_score)

        df_sc['TRADE_VOLUMN'] = df_sc['TRADE_VOLUMN'].map(float)
        df_sc['TRADE_VOLUMN'] = pd.cut(df_sc['TRADE_VOLUMN'], bins=self.trade_volumn_bin, right=False)
        df_sc['TRADE_VOLUMN'] = df_sc['TRADE_VOLUMN'].map(str).map(self.trade_volumn_score)

        df_sc['STAFF_NUM'] = df_sc['STAFF_NUM'].map(float)
        df_sc['STAFF_NUM'] = pd.cut(df_sc['STAFF_NUM'], bins=self.staff_bin, right=False)
        df_sc['STAFF_NUM'] = df_sc['STAFF_NUM'].map(str).map(self.staff_score)

        df_sc['INCOME'] = df_sc['INCOME'].map(float)
        df_sc['INCOME'] = pd.cut(df_sc['INCOME'], bins=self.income_bin, right=False)
        df_sc['INCOME'] = df_sc['INCOME'].map(str).map(self.income_score)

        df_sc['SHAREHOLDER_INCOME'] = df_sc['SHAREHOLDER_INCOME'].map(float)
        df_sc['SHAREHOLDER_INCOME'] = pd.cut(df_sc['SHAREHOLDER_INCOME'], bins=self.s_income_bin, right=False)
        df_sc['SHAREHOLDER_INCOME'] = df_sc['SHAREHOLDER_INCOME'].map(str).map(self.s_income_score)
        df_sc = df_sc.fillna(0)
        df_sc['SC_TOTAL_SCORE'] = df_sc.apply(
            lambda x: x['RATING'] + x['BVD_RATE'] + x['TRADE_TIMES'] + self.init_score + x['TRADE_TIMES'] + x[
                'TRADE_VOLUMN'] + x['STAFF_NUM'] + x['INCOME'] + x['SHAREHOLDER_INCOME'], axis=1)
        df_sc['SC_TOTAL_SCORE'] = df_sc['SC_TOTAL_SCORE'].apply(lambda x: 100 if x > 100 else x)
        df_sc['CUSTOMER_CODE'] = 'FTA_LG'

        Write_Oracle().write_oracle(_name_BD_RISK_RESULT_SUPPLYCHAIN_SC1, df_sc, org_code=None, alarm=None)

    #     更新预警表
        json_param = [
            {'word': '发现{}起交易次数偏低事件', 'col': 'TRADE_TIMES', 'score': -5},
            {'word': '发现{}起贸易金额偏低事件', 'col': 'TRADE_VOLUMN', 'score': -5},
            {'word': '发现{}起企业员工人数偏低事件', 'col': 'TRADE_VOLUMN', 'score': -3},
            {'word': '发现{}起经营金额偏低事件', 'col': 'TRADE_VOLUMN', 'score': -5},
            {'word': '发现{}起股东收入金额偏低事件', 'col': 'TRADE_VOLUMN', 'score': -5},
            {'word': '发现{}起BVD评级异常事件', 'col': 'TRADE_VOLUMN', 'score': -1},
            {'word': '发现{}起BVD破产评级异常事件', 'col': 'TRADE_VOLUMN', 'score': 5},
        ]

        def gen_alarm(i, param):
            word = param['word']
            score = param['score']
            col = param['col']
            try:
                num = \
                df_sc[(df_sc[str(col)] == score) & (df_sc['ORG_CODE'] == str(i))].groupby(['ORG_CODE'])[
                    'ID'].count().values[0]
                word = word.format(num)
                return word
            except Exception as e:
                pass

        def get(i):
            alarm_reason = []
            for param in json_param:
                alarm_reason.append(gen_alarm(i, param))
            return alarm_reason

        for i in df_sc['ORG_CODE'].unique():
            res = []
            for j in get(i):
                if j is not None:
                    try:
                        temp = {}
                        temp['CHECK_TIME'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        temp['ORG_CODE'] = i
                        temp['MODEL_CODE'] = 'SUPPLYCHAIN'
                        temp['CHILD_MODEL_CODE'] = 'SC1'
                        temp['ALARM_REASON'] = j
                        temp['ALARM_NUMBER'] = int(re.findall('发现(.*?)起.*', j)[0])
                        res.append(temp)
                    except Exception as e:
                        pass
            df_alarm = pd.DataFrame(res)
            if df_alarm.empty:
                pass
            else:
                df_alarm = df_alarm.reset_index().rename(columns={'index': 'ID'})
                df_alarm['CHECK_TIME'] = pd.to_datetime(df_alarm['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
                df_alarm['CUSTOMER_CODE'] = 'FTA_LG'
                Write_Oracle().write_oracle('BD_RISK_ALARM_ITEM', df_alarm, org_code=i,
                                            alarm=[self.model_code, self.child_model_code])
    def run_model_sc1(self):
        exec_status = None
        try:
            self.model_sc1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = 'f2c9ccd0e1fc4dd0ac21a838a8e75469'
    org_code, param_json, base_time = read_log_table(child_task_id)
    SupplyChainSc1(org_code=org_code, params=param_json, base_time=base_time,
                   child_task_id=child_task_id).run_model_sc1()
