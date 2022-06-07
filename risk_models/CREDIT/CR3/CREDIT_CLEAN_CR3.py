import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_CORP_INFO, _name_ZWY_DWCBQK_XXB, _name_WATER_RATE_USAGE, _name_ELECTRIC_CHARGE_USAGE, _name_NATURAL_GAS_USAGE, _name_FT_CUS_DWS_TRADE, _name_DIM_TRADER


class CreditCleanCr3(object):
    def __init__(self, child_task_id, org_code):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        
    def clean_cr3(self):
        # 获取企业潜在风险相关数据指标
        POTENTIAL_RISK_DETAIL = Read_Oracle().read_oracle(sql= 
        """ 
        -- 单位全部参保人数是否正常
        select TYSHXYM ORG_CODE, DWMC ORG_NAME, cbny INDEX_DATE, '员工参保率' INDEX_NAME, round(DWCBRS/ZZZGPJRS,2) INDEX_VALUE
        from {}
        where TYSHXYM = '{}'
        and ZZZGPJRS != 0
        
        -- 员工平均参保金额是否正常
        union all
        select TYSHXYM ORG_CODE, DWMC ORG_NAME, cbny INDEX_DATE, '员工平均参保金额' INDEX_NAME, round(JNJE/DWCBRS,2) INDEX_VALUE
        from {}
        where TYSHXYM = '{}'
        
        -- 参保人口结构是否稳定
        union all
        select TYSHXYM ORG_CODE, DWMC ORG_NAME, cbny INDEX_DATE, '参保人数本市户籍占比' INDEX_NAME, round((BSCZHJJFRS+BSNCHJJFRS)/DWCBRS,2) INDEX_VALUE
        from {}
        where TYSHXYM = '{}'
        
        -- 水费
        union all
        select uni_sc_id ORG_CODE, org_name ORG_NAME, stat_date INDEX_DATE, '应收水费' INDEX_NAME, round(replace(water_rate,','),2) INDEX_VALUE
        from {} t1
        left join
        (select uni_sc_id, corp_name
        from {} where iscurrent = 1) t2
        on t1.org_name = t2.corp_name
        where uni_sc_id = '{}'
        
        -- 电费
        union all
        select uni_sc_id ORG_CODE, org_name ORG_NAME, stat_date INDEX_DATE, '应收电费' INDEX_NAME, round(replace(electric_charge,','),2)  INDEX_VALUE
        from {} t3
        left join
        (select uni_sc_id, corp_name
        from {} where iscurrent = 1) t4
        on t3.org_name = t4.corp_name
        where uni_sc_id = '{}'
        
        -- 煤气费
        union all
        select uni_sc_id ORG_CODE, org_name ORG_NAME, stat_date INDEX_DATE, '应收煤气费' INDEX_NAME, round(replace(gas_consumption,','),2)  INDEX_VALUE
        from {} t5
        left join
        (select uni_sc_id, corp_name
        from {} where iscurrent = 1) t6
        on t5.org_name = t6.corp_name
        where uni_sc_id = '{}'
        """.format(_name_ZWY_DWCBQK_XXB, self.org_code, _name_ZWY_DWCBQK_XXB, self.org_code, _name_ZWY_DWCBQK_XXB, self.org_code, _name_WATER_RATE_USAGE, _name_CORP_INFO, self.org_code, _name_ELECTRIC_CHARGE_USAGE, _name_CORP_INFO, self.org_code, _name_NATURAL_GAS_USAGE, _name_CORP_INFO, self.org_code), database = 'dbods')
        
        # 加入企业信息; ID; 模型运行时间
        POTENTIAL_RISK_DETAIL['ID'] = range(len(POTENTIAL_RISK_DETAIL))
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        POTENTIAL_RISK_DETAIL['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        
        # 明确企业所属租户
        POTENTIAL_RISK_DETAIL['CUSTOMER_CODE'] = 'FTA_LG'
        
        # 整理明细表
        POTENTIAL_RISK_DETAIL = POTENTIAL_RISK_DETAIL[['ID', 'ORG_CODE', 'INDEX_DATE', 'INDEX_NAME', 'INDEX_VALUE', 'CHECK_TIME', 'CUSTOMER_CODE']]
        
        # 读入数据库
        Write_Oracle().write_oracle('BD_RISK_DETAIL_CREDIT_CR3',POTENTIAL_RISK_DETAIL, org_code = self.org_code, alarm = None)
        
        # 用于前端画图
        CBRS = Read_Oracle().read_oracle(sql= 
            """ 
            select TYSHXYM ORG_CODE, cbny STA_DATE, DWCBRS CBRS, '位' CBRS_UNIT
            from {}
            where TYSHXYM = '{}'
            """.format(_name_ZWY_DWCBQK_XXB, self.org_code), database = 'dbods')
        
        ELECTRIC = Read_Oracle().read_oracle(sql= 
            """ 
            select uni_sc_id ORG_CODE, stat_date STA_DATE, round(replace(electric_charge,','),2) ELECTRIC, ELECTRIC_CHARGE_UNIT ELECTRIC_UNIT
            from {} t3
            left join
            (select uni_sc_id, corp_name
            from {} where iscurrent = 1) t4
            on t3.org_name = t4.corp_name
            where uni_sc_id = '{}'
            """.format(_name_ELECTRIC_CHARGE_USAGE, _name_CORP_INFO, self.org_code), database = 'dbods')
        
        I_E_VALUE = Read_Oracle().read_oracle(sql= 
            """ 
            with corp_goods as (select trade_code_scc, substr(DECL_DATE_KEY,1,6) as date_month, SUM_GOODS_GROSS_VALUE_RMB
            from {} t1
            left join 
            {} t2 
            on t1.trade_code_key = t2.trade_key 
            where trade_code_scc = '{}')
            select trade_code_scc ORG_CODE, date_month STA_DATE, round(sum(SUM_GOODS_GROSS_VALUE_RMB)/10000, 2) GOODS_VALUE, '万元' goods_unit
            from corp_goods
            where date_month >= '201801'
            group by trade_code_scc, date_month
            order by date_month
            """.format(_name_FT_CUS_DWS_TRADE, _name_DIM_TRADER, self.org_code), database = 'dbdw')
        
        CREDIT_GRAPH = pd.merge(I_E_VALUE, CBRS, left_on=['ORG_CODE', 'STA_DATE'], right_on=['ORG_CODE', 'STA_DATE'], how='left')
        CREDIT_GRAPH = pd.merge(CREDIT_GRAPH, ELECTRIC, left_on=['ORG_CODE', 'STA_DATE'], right_on=['ORG_CODE', 'STA_DATE'], how='left')
        
        # 加入企业信息; ID; 模型运行时间
        CREDIT_GRAPH['ID'] = range(len(CREDIT_GRAPH))
        CREDIT_GRAPH['CHECK_TIME'] = datetime.datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        
        # 明确企业所属租户
        CREDIT_GRAPH['CUSTOMER_CODE'] = 'FTA_LG'
        
        # 整理明细表
        CREDIT_GRAPH = CREDIT_GRAPH[['ID', 'ORG_CODE', 'STA_DATE', 'GOODS_VALUE', 'GOODS_UNIT', 'CBRS', 'CBRS_UNIT', 'ELECTRIC', 'ELECTRIC_UNIT', 'CHECK_TIME','CUSTOMER_CODE']]
        
        # 读入数据库
        Write_Oracle().write_oracle('BD_RISK_GRAPH_CREDIT_CR3',CREDIT_GRAPH, org_code = self.org_code, alarm = None)

    def run_clean_cr3(self):
        # insert into bd_risk_model_log values (29, '1', '0001_0030', 'CREDIT', 'CR3', '91310115688774070B', sysdate,'' , '{}','' ,'' , sysdate, sysdate, sysdate)
        try:
            self.clean_cr3()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id = self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '0001_0030'
    org_code, params, base_time = read_log_table(child_task_id)
    CreditCleanCr3(child_task_id, org_code).run_clean_cr3()