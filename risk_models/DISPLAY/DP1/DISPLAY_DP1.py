import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_FT_I_DTL_SEA_PRE_RECORDED, _name_FT_I_DTL_SEA_LIST, _name_FT_I_DTL_OTR_PRE_RECORDED, _name_FT_I_DTL_OTR_LIST, _name_FT_E_DTL_SEA_PRE_RECORDED, _name_FT_E_DTL_SEA_LIST, _name_FT_E_DTL_OTR_PRE_RECORDED, _name_FT_E_DTL_OTR_LIST, _name_DIM_FTZ_CORP, _name_DIM_TRADER


class DisplayDp1(object):
    def __init__(self, child_task_id, base_time):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status=None)

        # 参数读取
        self.base_time = base_time.replace('-','')
        self.base_time = self.base_time.replace('/','')
        self.base_time = self.base_time[0:8]

    def date_standard(self, df):
        """
        des: 填充缺失的时间
        input df['col']
        output df_series
        """
        b = []
        date_begin = df.min()
        date_end = df.max()
        a = pd.date_range(date_begin, date_end, freq='MS', normalize=True)
        for i in a:
            i = str(i)
            i = datetime.datetime.strptime(i, '%Y-%m-%d %H:%M:%S')
            b.append(i)
        date_serie = pd.DataFrame(b)
        date_serie = date_serie.rename(columns={0: 'STANDARD_DATE'})
        return date_serie

    def model_dp1(self):
        # 计算统计起始时间（当前时间的去年1月份）
        start_time = str( int(self.base_time[0:4]) - 1 ) + '0101'

        # 进口报关单货值
        sql_I = '''
        SELECT 'I' AS IE_FLAG,
        T1.TIME AS DATETIME,
        SUM(T1.ENTRY_NUM) AS ENTRY_NUM,
        SUM(T1.RMB)/100000000 AS RMB,
        SUM(T1.TRADE_NUM) AS TRADE_NUM,
        '报关单' as type,
        (SELECT count(distinct corp_uid) FROM {} where zmxpq_flag = 1) as corp_num
        FROM 
        (SELECT TO_DATE(SUBSTR(A.APL_DATE_KEY,1,6),'YYYYMM') AS TIME , 
        COUNT(DISTINCT A.PRE_ENTRY_NO) AS ENTRY_NUM,SUM(B.GOODS_GROSS_VALUE_RMB) AS RMB ,COUNT(DISTINCT A.TRADE_CODE) AS TRADE_NUM
        FROM {} A 
        JOIN {} B ON A.ID =B.HEAD_ID
        WHERE A.APL_DATE_KEY >= {} AND A.decl_date <= TRUNC(LAST_DAY(ADD_MONTHS(SYSDATE, -1)))  AND A.CUSTOMS_CODE ='2249'
        AND A.TRADE_CODE IN (select distinct trade_code from {} where trade_code_scc in (select uni_sc_id FROM {} where zmxpq_flag = 1))
        GROUP BY SUBSTR(A.APL_DATE_KEY,1,6)
        union all
        SELECT TO_DATE(SUBSTR(C.APL_DATE_KEY,1,6),'YYYYMM') AS TIME , 
        COUNT(DISTINCT C.PRE_ENTRY_NO) AS ENTRY_NUM,SUM(D.GOODS_GROSS_VALUE_RMB) AS RMB,COUNT(DISTINCT C.TRADE_CODE) AS TRADE_NUM
        FROM {} C 
        JOIN {} D ON C.ID =D.HEAD_ID
        WHERE C.APL_DATE_KEY >= {} AND C.decl_date <= TRUNC(LAST_DAY(ADD_MONTHS(SYSDATE, -1)))  AND C.CUSTOMS_CODE ='2249'
        AND C.TRADE_CODE IN (select distinct trade_code from {} where trade_code_scc in (select uni_sc_id FROM {} where zmxpq_flag = 1))
        GROUP BY SUBSTR(C.APL_DATE_KEY,1,6)) T1
        GROUP BY T1.TIME
        ORDER BY TIME
        '''.format(_name_DIM_FTZ_CORP, _name_FT_I_DTL_SEA_PRE_RECORDED, _name_FT_I_DTL_SEA_LIST, start_time, _name_DIM_TRADER, _name_DIM_FTZ_CORP, 
                   _name_FT_I_DTL_OTR_PRE_RECORDED, _name_FT_I_DTL_OTR_LIST, start_time, _name_DIM_TRADER, _name_DIM_FTZ_CORP)
        t1 = Read_Oracle().read_oracle(sql=sql_I, database='dbdw')
        
        # 时间字段标准化
        t1['DATETIME'] = self.date_standard(t1['DATETIME'])
        
        # 出口报关单货值
        sql_E = '''
        SELECT 'E' AS IE_FLAG,
        T1.TIME AS DATETIME,
        SUM(T1.ENTRY_NUM) AS ENTRY_NUM,
        SUM(T1.RMB)/100000000 AS RMB,
        SUM(T1.TRADE_NUM) AS TRADE_NUM,
        '报关单' as type,
        (SELECT count(distinct corp_uid) FROM {} where zmxpq_flag = 1) as corp_num
        FROM 
        (SELECT TO_DATE(SUBSTR(A.APL_DATE_KEY,1,6),'YYYYMM') AS TIME , 
        COUNT(DISTINCT A.PRE_ENTRY_NO) AS ENTRY_NUM,SUM(B.GOODS_GROSS_VALUE_RMB) AS RMB ,COUNT(DISTINCT A.TRADE_CODE) AS TRADE_NUM
        FROM {} A 
        JOIN {} B ON A.ID =B.HEAD_ID
        WHERE A.APL_DATE_KEY >= {} AND A.decl_date <= TRUNC(LAST_DAY(ADD_MONTHS(SYSDATE, -1)))  AND A.CUSTOMS_CODE ='2249'
        AND A.TRADE_CODE IN (select distinct trade_code from {} where trade_code_scc in (select uni_sc_id FROM {} where zmxpq_flag = 1))
        GROUP BY SUBSTR(A.APL_DATE_KEY,1,6)
        union all
        SELECT TO_dATE(SUBSTR(C.APL_DATE_KEY,1,6),'YYYYMM') AS TIME , 
        COUNT(DISTINCT C.PRE_ENTRY_NO) AS ENTRY_NUM,SUM(D.GOODS_GROSS_VALUE_RMB) AS RMB,COUNT(DISTINCT C.TRADE_CODE) AS TRADE_NUM
        FROM {} C 
        JOIN {} D ON C.ID =D.HEAD_ID
        WHERE C.APL_DATE_KEY >= {} AND C.decl_date <= TRUNC(LAST_DAY(ADD_MONTHS(SYSDATE, -1)))  AND C.CUSTOMS_CODE ='2249'
        AND C.TRADE_CODE IN (select distinct trade_code from {} where trade_code_scc in (select uni_sc_id FROM {} where zmxpq_flag = 1))
        GROUP BY SUBSTR(C.APL_DATE_KEY,1,6)) T1
        GROUP BY T1.TIME
        ORDER BY TIME
        '''.format(_name_DIM_FTZ_CORP, _name_FT_E_DTL_SEA_PRE_RECORDED, _name_FT_E_DTL_SEA_LIST, start_time, _name_DIM_TRADER, _name_DIM_FTZ_CORP, 
                   _name_FT_E_DTL_OTR_PRE_RECORDED, _name_FT_E_DTL_OTR_LIST, start_time, _name_DIM_TRADER, _name_DIM_FTZ_CORP)
        t2 = Read_Oracle().read_oracle(sql=sql_E, database='dbdw')
        
        # 时间字段标准化
        t2['DATETIME'] = self.date_standard(t2['DATETIME'])
        
        # 合并进出口数据并添加ID字段
        df_1 = pd.concat([t1, t2])
        df_1.reset_index(inplace=True)
        df_1['ID'] = range(len(df_1))
        df_1 = df_1[['ID', 'IE_FLAG', 'DATETIME', 'ENTRY_NUM', 'RMB', 'TRADE_NUM', 'TYPE', 'CORP_NUM']]
        
        Write_Oracle().write_oracle('BD_RISK_DISPLAY_MAIN',df_1,org_code = None, alarm=None)

    def run_model_dp1(self):
        try:
            self.model_dp1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id = self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '0001_0001'
    org_code, param_json, base_time = read_log_table(child_task_id)
    DisplayDp1(child_task_id, base_time).run_model_dp1()

