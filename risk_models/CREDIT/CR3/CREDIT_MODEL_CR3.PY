import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_DETAIL_CREDIT_CR3


class CreditModelCr3(object):
    def __init__(self, child_task_id, org_code, params):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.ratio = json.loads(params)['ratio']
        self.cbl = json.loads(params)['cbl']

    def model_cr3(self):
        # 读取明细表
        POTENTIAL_RISK_RESULT = Read_Oracle().read_oracle(sql= """ select * from {} where org_code = '{}' and iscurrent = 1 and CUSTOMER_CODE = 'FTA_LG' """.format(_name_BD_RISK_DETAIL_CREDIT_CR3, self.org_code), database = 'dbdm')
        
        # 阈值计算函数
        def Qfunc(df):
            Q1 = np.percentile(df['INDEX_VALUE'], 25)
            Q3 = np.percentile(df['INDEX_VALUE'], 75)
            IQR = Q3 - Q1
            outlier_step = 1.5 * IQR
            return (Q3 + outlier_step), (Q1 - outlier_step)
        
        # 计算上下阈值
        cutoff = POTENTIAL_RISK_RESULT.groupby(['ORG_CODE', 'INDEX_NAME']).apply(Qfunc)
        cutoff = pd.DataFrame(cutoff).reset_index()
        cutoff['CUTOFF_HIGH'] = cutoff[0].map(lambda x: x[0])
        cutoff['CUTOFF_LOW'] = cutoff[0].map(lambda x: x[1])
        cutoff.drop(0, axis=1, inplace=True)
        POTENTIAL_RISK_RESULT = pd.merge(POTENTIAL_RISK_RESULT, cutoff, on=['ORG_CODE','INDEX_NAME'], how='left')
        
        # 打标签（'员工参保率'不采用IQR阈值，使用参数固定阈值）
        POTENTIAL_RISK_RESULT['RISK_LABEL'] = POTENTIAL_RISK_RESULT.loc[POTENTIAL_RISK_RESULT.INDEX_NAME == '员工参保率','INDEX_VALUE'].map(lambda x: '员工参保率不足' if x < self.cbl else '员工参保率正常')
        
        # 其余5类标签(采用IQR阈值)
        # 平均参保金额判断
        if POTENTIAL_RISK_RESULT[POTENTIAL_RISK_RESULT.INDEX_NAME == '员工平均参保金额'].empty is True:
            print('找不到该企业员工平均参保金额数据')
        else:
            cutoff_low_1 = np.unique(cutoff.loc[cutoff.INDEX_NAME =='员工平均参保金额','CUTOFF_LOW'])[0]
            cutoff_high_1 = np.unique(cutoff.loc[cutoff.INDEX_NAME =='员工平均参保金额','CUTOFF_HIGH'])[0]
            POTENTIAL_RISK_RESULT.loc[POTENTIAL_RISK_RESULT.INDEX_NAME == '员工平均参保金额','RISK_LABEL'] = POTENTIAL_RISK_RESULT.loc[POTENTIAL_RISK_RESULT.INDEX_NAME == '员工平均参保金额','INDEX_VALUE'].map(lambda x: '人均参保金较低' if x < cutoff_low_1 else ('人均参保金较高' if x > cutoff_high_1 else '人均参保金额稳定'))
        
        # 参保人数本市户籍占比判断
        if POTENTIAL_RISK_RESULT[POTENTIAL_RISK_RESULT.INDEX_NAME == '参保人数本市户籍占比'].empty is True:
            print('找不到该企业参保人数本市户籍占比数据')
        else:
            cutoff_low_2 = np.unique(cutoff.loc[cutoff.INDEX_NAME =='参保人数本市户籍占比','CUTOFF_LOW'])[0]
            cutoff_high_2 = np.unique(cutoff.loc[cutoff.INDEX_NAME =='参保人数本市户籍占比','CUTOFF_HIGH'])[0]
            POTENTIAL_RISK_RESULT.loc[POTENTIAL_RISK_RESULT.INDEX_NAME == '参保人数本市户籍占比','RISK_LABEL'] = POTENTIAL_RISK_RESULT.loc[POTENTIAL_RISK_RESULT.INDEX_NAME == '参保人数本市户籍占比','INDEX_VALUE'].map(lambda x: '本市户籍人数异常下降' if x < cutoff_low_2 else ('本市户籍人数异常上升' if x > cutoff_high_2 else '本市户籍人数稳定'))
    
        # 应收水费判断
        if POTENTIAL_RISK_RESULT[POTENTIAL_RISK_RESULT.INDEX_NAME == '应收水费'].empty is True:
            print('找不到该企业应收水费数据')
        else:
            cutoff_low_3 = np.unique(cutoff.loc[cutoff.INDEX_NAME =='应收水费','CUTOFF_LOW'])[0]
            cutoff_high_3 = np.unique(cutoff.loc[cutoff.INDEX_NAME =='应收水费','CUTOFF_HIGH'])[0]
            POTENTIAL_RISK_RESULT.loc[POTENTIAL_RISK_RESULT.INDEX_NAME == '应收水费','RISK_LABEL'] = POTENTIAL_RISK_RESULT.loc[POTENTIAL_RISK_RESULT.INDEX_NAME == '应收水费','INDEX_VALUE'].map(lambda x: '水资源消耗异常下降' if x < cutoff_low_3 else ('水资源消耗异常上升' if x > cutoff_high_3 else '水资源消耗水平正常'))
        
        # 应收电费判断
        if POTENTIAL_RISK_RESULT[POTENTIAL_RISK_RESULT.INDEX_NAME == '应收电费'].empty is True:
            print('找不到该企业应收电费数据')
        else:
            cutoff_low_4 = np.unique(cutoff.loc[cutoff.INDEX_NAME =='应收电费','CUTOFF_LOW'])[0]
            cutoff_high_4 = np.unique(cutoff.loc[cutoff.INDEX_NAME =='应收电费','CUTOFF_HIGH'])[0]
            POTENTIAL_RISK_RESULT.loc[POTENTIAL_RISK_RESULT.INDEX_NAME == '应收电费','RISK_LABEL'] = POTENTIAL_RISK_RESULT.loc[POTENTIAL_RISK_RESULT.INDEX_NAME == '应收电费','INDEX_VALUE'].map(lambda x: '电量消耗异常下降' if x < cutoff_low_4 else ('电量消耗异常上升' if x > cutoff_high_4 else '电量消耗水平正常'))
        
        # 应收煤气费判断
        if POTENTIAL_RISK_RESULT[POTENTIAL_RISK_RESULT.INDEX_NAME == '应收煤气费'].empty is True:
            print('找不到该企业应收煤气费数据')
        else:
            cutoff_low_5 = np.unique(cutoff.loc[cutoff.INDEX_NAME =='应收煤气费','CUTOFF_LOW'])[0]
            cutoff_high_5 = np.unique(cutoff.loc[cutoff.INDEX_NAME =='应收煤气费','CUTOFF_HIGH'])[0]
            POTENTIAL_RISK_RESULT.loc[POTENTIAL_RISK_RESULT.INDEX_NAME == '应收煤气费','RISK_LABEL'] = POTENTIAL_RISK_RESULT.loc[POTENTIAL_RISK_RESULT.INDEX_NAME == '应收煤气费','INDEX_VALUE'].map(lambda x: '煤气消耗异常下降' if x < cutoff_low_5 else ('煤气消耗异常上升' if x > cutoff_high_5 else '煤气消耗水平正常'))
        
        
        # 算分
        ratio = float(eval(self.ratio))
        good_sample = ['员工参保率正常', '人均参保金额稳定', '本市户籍人数稳定', '水资源消耗水平正常', '电量消耗水平正常', '煤气消耗水平正常']
        # 计算分数
        POTENTIAL_RISK_RESULT['SCORE'] = POTENTIAL_RISK_RESULT['RISK_LABEL'].map(lambda x: 0.00 if x in good_sample else ratio)
        
        # 更新ID; 模型运行时间
        POTENTIAL_RISK_RESULT['ID'] = range(len(POTENTIAL_RISK_RESULT))
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        POTENTIAL_RISK_RESULT['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        
        # 明确企业所属租户
        POTENTIAL_RISK_RESULT['CUSTOMER_CODE'] = 'FTA_LG'
        
        # 整理结果表
        POTENTIAL_RISK_RESULT = POTENTIAL_RISK_RESULT[['ID','ORG_CODE','INDEX_DATE','INDEX_NAME','RISK_LABEL','SCORE','CHECK_TIME','CUSTOMER_CODE']]
        
        # 读入数据库
        Write_Oracle().write_oracle('BD_RISK_RESULT_CREDIT_CR3',POTENTIAL_RISK_RESULT, org_code=self.org_code, alarm = None)
        
        # 整理预警明细数据，并写入数据库
        RISK_ALARM = POTENTIAL_RISK_RESULT[(POTENTIAL_RISK_RESULT.RISK_LABEL != '员工参保率正常')&(POTENTIAL_RISK_RESULT.RISK_LABEL != '人均参保金额稳定')&(POTENTIAL_RISK_RESULT.RISK_LABEL != '本市户籍人数稳定')&(POTENTIAL_RISK_RESULT.RISK_LABEL != '水资源消耗水平正常')&(POTENTIAL_RISK_RESULT.RISK_LABEL != '电量消耗水平正常')&(POTENTIAL_RISK_RESULT.RISK_LABEL != '煤气消耗水平正常')].groupby(['RISK_LABEL'], as_index=False)['ID'].count()
        RISK_ALARM = RISK_ALARM.rename(columns={'ID':'ALARM_NUMBER'})
        RISK_ALARM['ALARM_REASON'] = '发现' + RISK_ALARM['ALARM_NUMBER'].astype('str') + '起' + RISK_ALARM['RISK_LABEL'] + '事件'
        RISK_ALARM['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        RISK_ALARM['ORG_CODE'] = self.org_code
        RISK_ALARM['MODEL_CODE'] = 'CREDIT'
        RISK_ALARM['CHILD_MODEL_CODE'] = 'CR3'
        RISK_ALARM['ID'] = range(len(RISK_ALARM))
        # 明确企业所属租户
        RISK_ALARM['CUSTOMER_CODE'] = 'FTA_LG'
        RISK_ALARM = RISK_ALARM[['ID','ORG_CODE','MODEL_CODE','CHILD_MODEL_CODE','ALARM_REASON','ALARM_NUMBER','CHECK_TIME','CUSTOMER_CODE']]
        
        if RISK_ALARM.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle('BD_RISK_ALARM_ITEM',RISK_ALARM, org_code = self.org_code, alarm = ['CREDIT','CR3'])
    
    
    def run_model_cr3(self):
        try:
            self.model_cr3()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id = self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '0002_0030'
    org_code, param_json, base_time = read_log_table(child_task_id)
    CreditModelCr3(child_task_id, org_code, params=param_json).run_model_cr3()
