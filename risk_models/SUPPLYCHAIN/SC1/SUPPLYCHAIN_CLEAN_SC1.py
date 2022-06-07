import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_FT_I_DTL_SEA_PRE_RECORDED, \
    _name_FT_I_DTL_SEA_CONTAINER, _name_FT_I_DTL_COARRI_CTNR, _name_DIM_OPERATOR, _name_DIM_FTZ_CORP, \
    _name_FT_E_DTL_SEA_PRE_RECORDED, \
    _name_FT_E_DTL_SEA_CONTAINER, _name_FT_E_DTL_COARRI_CTNR, _name_MX_BVD, _name_FT_I_DTL_SEA_LIST, _name_FT_E_DTL_SEA_LIST,\
    _name_BD_RISK_DETAIL_SUPPLYCHAIN_SC1


class SupplyChainSc1(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'SUPPLYCHAIN'
        self.child_model_code = 'SC1'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.params = json.loads(params)
        self.startdt = (dt_method.strptime(base_time, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=182)) \
            .strftime('%Y-%m-%d')
        self.enddt = (dt_method.strptime(base_time, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=31)) \
            .strftime('%Y-%m-%d')
    def clean_sc1(self):
        startdt = self.startdt
        enddt = self.enddt
        sql1 = f"""
                SELECT 
                TRADE_NAME,
                E.UNI_SC_ID as ORG_CODE,
                GOODS_OWNER,
                CUS_DECL_AGENT,
                OVERSEAS_CONSIGNOR_NAME_EN,
                D.OPR_NAME_CN,
                SUM(GOODS_GROSS_VALUE_RMB) AS SUM_RMB
                FROM {_name_FT_I_DTL_SEA_PRE_RECORDED} A
                JOIN {_name_FT_I_DTL_SEA_CONTAINER} B  ON A.ID=B.HEAD_id
                JOIN  {_name_FT_I_DTL_SEA_LIST} F ON F.HEAD_ID = A.ID
                LEFT JOIN {_name_FT_I_DTL_COARRI_CTNR} C ON B.VSL_NAME=C.VSL_NAME
                AND B.VOYAGE=C.VOYAGE AND B.CTNR_NO=C.CTNR_NO
                LEFT JOIN {_name_DIM_OPERATOR} D ON C.CTNR_OPR_CODE=D.OPR_CODE
                inner join {_name_DIM_FTZ_CORP} E on a.trade_name = e.corp_name AND e.zmxpq_flag = 1
                where a.decl_date>=to_date('{startdt}','yyyy-mm-dd')
                AND a.decl_date<to_date('{enddt}','yyyy-mm-dd')
                GROUP BY TRADE_NAME,UNI_SC_ID,GOODS_OWNER,CUS_DECL_AGENT,OVERSEAS_CONSIGNOR_NAME_EN,OPR_NAME_CN
                """
        sql2 = f"""
                SELECT 
                TRADE_NAME,
                E1.UNI_SC_ID as ORG_CODE,
                GOODS_OWNER,
                CUS_DECL_AGENT,
                OVERSEAS_CONSIGNOR_NAME_EN,
                D1.OPR_NAME_CN,
                SUM(GOODS_GROSS_VALUE_RMB) AS SUM_RMB
                FROM {_name_FT_E_DTL_SEA_PRE_RECORDED} A1
                JOIN {_name_FT_E_DTL_SEA_CONTAINER} B1 ON A1.ID=B1.HEAD_id
                JOIN  {_name_FT_E_DTL_SEA_LIST} F1 ON F1.HEAD_ID = A1.ID
                LEFT JOIN {_name_FT_E_DTL_COARRI_CTNR} C1 ON B1.VSL_NAME=C1.VSL_NAME
                AND B1.VOYAGE=C1.VOYAGE AND B1.CTNR_NO=C1.CTNR_NO
                LEFT JOIN {_name_DIM_OPERATOR} D1 ON C1.CTNR_OPR_CODE=D1.OPR_CODE
                inner join {_name_DIM_FTZ_CORP} E1 on a1.trade_name = e1.corp_name AND e1.zmxpq_flag = 1
                where a1.decl_date>=to_date('{startdt}','yyyy-mm-dd')
                AND a1.decl_date<to_date('{enddt}','yyyy-mm-dd')
                GROUP BY TRADE_NAME,UNI_SC_ID,GOODS_OWNER,CUS_DECL_AGENT,OVERSEAS_CONSIGNOR_NAME_EN,OPR_NAME_CN
                """
        df_sc1 = Read_Oracle().read_oracle(sql=sql1, database='dbdw')
        df_sc2 = Read_Oracle().read_oracle(sql=sql2, database='dbdw')
        df_sc = pd.concat([df_sc1, df_sc2], axis=0)
        df_sc.rename(columns={'SUM_RMB': 'TRADE_VOLUMN'}, inplace=True)
        df_sc['TRADE_TIMES'] = 1
        df_goods_owner = df_sc.groupby(['TRADE_NAME','ORG_CODE', 'GOODS_OWNER'])['TRADE_TIMES'].count().reset_index()
        df_goods_owner_2 = df_sc.groupby(['TRADE_NAME','ORG_CODE', 'GOODS_OWNER'])['TRADE_VOLUMN'].sum().reset_index()
        df_goods_owner = df_goods_owner.merge(df_goods_owner_2, how='left', on=['TRADE_NAME', 'ORG_CODE','GOODS_OWNER'])
        df_goods_owner['TYPE'] = '货主'
        df_goods_owner.rename(columns={'GOODS_OWNER': 'SC_COMPANY'}, inplace=True)
        df_agent = df_sc.groupby(['TRADE_NAME','ORG_CODE', 'CUS_DECL_AGENT'])['TRADE_TIMES'].count().reset_index()
        df_agent_2 = df_sc.groupby(['TRADE_NAME','ORG_CODE', 'CUS_DECL_AGENT'])['TRADE_VOLUMN'].sum().reset_index()
        df_agent = df_agent.merge(df_agent_2, how='left', on=['TRADE_NAME','ORG_CODE', 'CUS_DECL_AGENT'])
        df_agent['TYPE'] = '报关行'
        df_agent.rename(columns={'CUS_DECL_AGENT': 'SC_COMPANY'}, inplace=True)
        df_oversea = df_sc.groupby(['TRADE_NAME','ORG_CODE', 'OVERSEAS_CONSIGNOR_NAME_EN'])['TRADE_TIMES'].count().reset_index()
        df_oversea_2 = df_sc.groupby(['TRADE_NAME','ORG_CODE', 'OVERSEAS_CONSIGNOR_NAME_EN'])['TRADE_VOLUMN'].sum().reset_index()
        df_oversea = df_oversea.merge(df_oversea_2, how='left', on=['TRADE_NAME','ORG_CODE', 'OVERSEAS_CONSIGNOR_NAME_EN'])
        df_oversea['TYPE'] = '境外收发货人'
        df_oversea.rename(columns={'OVERSEAS_CONSIGNOR_NAME_EN': 'SC_COMPANY'}, inplace=True)
        df_ship_opr = df_sc.groupby(['TRADE_NAME','ORG_CODE', 'OPR_NAME_CN'])['TRADE_TIMES'].count().reset_index()
        df_ship_opr_2 = df_sc.groupby(['TRADE_NAME','ORG_CODE', 'OPR_NAME_CN'])['TRADE_VOLUMN'].sum().reset_index()
        df_ship_opr = df_ship_opr.merge(df_ship_opr_2, how='left', on=['TRADE_NAME','ORG_CODE', 'OPR_NAME_CN'])
        df_ship_opr['TYPE'] = '船公司'
        df_ship_opr.rename(columns={'OPR_NAME_CN': 'SC_COMPANY'}, inplace=True)
        # 链接所有表CONCAT

        frames = [df_goods_owner, df_agent, df_oversea, df_ship_opr]
        df_supply_chain = pd.concat(frames, axis=0)
        sql = f"""select corp_name,rating,bvd_rate,STAFF_NUM,INCOME,SHAREHOLDER_INCOME from {_name_MX_BVD}"""
        df_bvd = Read_Oracle().read_oracle(sql=sql, database='dbods')
        df_bvd['TRADE_NAME'] = '上海飞机制造有限公司'
        df_bvd['ORG_CODE'] = '91310000132612172J'
        df = pd.merge(df_supply_chain, df_bvd, left_on=['TRADE_NAME', 'SC_COMPANY','ORG_CODE'],
                      right_on=['TRADE_NAME', 'CORP_NAME','ORG_CODE'], how='outer')
        df['SC_COMPANY'] = df['SC_COMPANY'].fillna('')
        df.loc[df['SC_COMPANY'] == '', 'SC_COMPANY'] = df.loc[df['SC_COMPANY'] == '', 'CORP_NAME']
        df.loc[df['TYPE'].isna(), 'TRADE_TIMES'] = 0
        df.loc[df['TYPE'].isna(), 'TYPE'] = '境外收发货人'
        df.drop(['CORP_NAME'], axis=1, inplace=True)
        df.fillna(0, inplace=True)
        now = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df['CHECK_TIME'] = now
        df['CHECK_TIME'] = df['CHECK_TIME'].astype('datetime64')
        def transfer(df, columns_list, dtype):
            for col in columns_list:
                df['{}'.format(col)] = df['{}'.format(col)].astype(dtype)

        transfer(df, ['TRADE_TIMES', 'TRADE_VOLUMN', 'BVD_RATE', 'STAFF_NUM','INCOME','SHAREHOLDER_INCOME'], float)
        BD_RISK_DETAIL_SUPPLYCHAIN_SC1 = df.reset_index().rename(columns={'index': 'ID'})
        BD_RISK_DETAIL_SUPPLYCHAIN_SC1['CUSTOMER_CODE'] = 'FTA_LG'

        Write_Oracle().write_oracle(_name_BD_RISK_DETAIL_SUPPLYCHAIN_SC1, BD_RISK_DETAIL_SUPPLYCHAIN_SC1, org_code=None,
                                    alarm= None)

    def run_clean_sc1(self):
        try:
            self.clean_sc1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    child_task_id = sys.argv[1]
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    # child_task_id = 'childtaskidsc1001'
    org_code, param_json, base_time = read_log_table(child_task_id)
    SupplyChainSc1(org_code=org_code, params=param_json, base_time=base_time,
                   child_task_id=child_task_id).run_clean_sc1()
