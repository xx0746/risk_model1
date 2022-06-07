import sys, os
from os import path

sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# sys.path.append('C:\\Users\\Administrator\\Desktop\\风控产品\\risk_project')
# sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *


class DisplayDp4(object):
    def __init__(self, org_code, base_time, child_task_id, param_json):
        self.model_code = 'DISPLAY'
        self.child_model_code = 'DP4'
        self.child_task_id = child_task_id
        self.org_code = org_code
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

    def model_dp4(self):
        org_code = self.org_code

        def get_tag(tagid):
            '''
            :param tagid: 标签id
            :return: 标签值
            '''
            try:
                tag = df_tag.loc[df_tag['GP_FTTAGCONFIG_FK'] == tagid, 'TAG_VALUE'].iloc[0]
            except:
                tag = ''
            return tag

        def dw_corp_info():
            '''
            从dw_corp_info表获取数据，这张表目前是新片区71家企业信息的汇总表
            # TODO 当新加企业的时候确保这个找唐荣山先更新
            :return:
            '''
            sql = f"""
            select CORP_NAME, CORP_TYPE,PERSON_NAME,ADDRESS,AREA_CODE,ZIP,TELEPHONE,ESTABLISH_DATE,
            CURRENCY,to_char(BUSINESS_SCOPE),PERSON_CERT_TYPE,PERSON_CERT_CODE,INDUSTRY_CODE,REG_NO,
            RECEIVING_ORGAN,CHANGE_DATE,CHANGE_ITEM,BUSINESS_ADDRESS,UNI_SC_ID,CORP_STATUS,REG_CAPITAL,
            PERSON_EMAIL,PERSON_LANDLINE_TEL,SUB_OBJ,CONTACT_MOBILE,CORP_COUNTRY,CONTACT_EMAIL,
            CONTACT_CER_TYPE,CONTACT_CER_NO
            from DW_LGXC_BASIC.FT_GOV_DTL_CORP_INFO
            where UNI_SC_ID = '{org_code}'
            """
            df = Read_Oracle().read_oracle(sql=sql, database='dbdw')
            return df

        def ods_cbrs():
            """
            Des
                ODS层的参保人数数据
                # DWCBRS 参保人数
                # DWMC 单位名称
                # JNJE 缴纳金额
                # SFQJ 是否欠缴
                # CBNY 年月
                # ZZZGPJRS 在职职工年平均人数
            """
            sql = """
            select DWCBRS, DWMC, JNJE, SFQJ, CBNY, ZZZGPJRS
            from DW_LGXC_BASIC.FT_GOV_DTL_ZWY_DWCBQK_XXB
            """
            df = Read_Oracle().read_oracle(sql=sql, database='dbdw')
            return df

        def dw_dim_industry_code():
            """
            Des
                获取行业代码
                # HY_DM 行业代码
                # HYMC 行业类型
            """
            sql = """
            select * from DIM.DIM_DM_GY_HY
            """
            return Read_Oracle().read_oracle(sql=sql, database='dbdw')

        def ods_corp_info_ext():
            """
            Des
                获取企业信息
            """
            sql = """
            select * from DW_LGXC_BASIC.FT_GOV_DTL_CORP_EXT
            """
            return Read_Oracle().read_oracle(sql=sql, database='dbdw')

        def ods_tax_level():
            """
            Des
                获取企业的纳税等级
            """
            sql = '''
            SELECT
                t2.NSRMC,
                t2.SHXYDM,
                t1.PDDJ 
                FROM DW_LGXC_BASIC.FT_GOV_DTL_SW_XYDJ t1
                LEFT JOIN DW_LGXC_BASIC.FT_GOV_DTL_SW_DJ_NSRXX t2
                ON t1.DJXH = t2.DJXH
                WHERE PDND=2018
            '''
            return Read_Oracle().read_oracle(sql=sql, database='dbdw')

        def ods_cos_level():
            sql = '''
            SELECT
              v.CORP_NAME,
              T.UNI_SC_ID,
              MTYPE FROM ODS_ZMXPQ.VIEW_CUST_MTYPE v
            LEFT JOIN DIM.map_ftz_corp_id t
              ON v.CORP_UID = t.CORP_UID
            WHERE v.REPEAL_DATE IS NULL'''
            return Read_Oracle().read_oracle(sql=sql, database='dbods')

        global df_tag
        sql = f'''select * from DW_CORP_BASIC.FT_TAG_CORP  A LEFT JOIN DW_CORP_BASIC.DW_CORP_UID B
        ON A.ENT_BDC_UNI_ID = B.ENT_BDC_UNI_ID
        where b.UNF_SC_ID ='{org_code}'
        and b.isdeleted = 0 '''
        df_tag = Read_Oracle().read_oracle(sql=sql, database='dbdw')
        # I_E_METHOD 进出口方式
        tag1 = get_tag(34)
        # CUS_QUA 报关资格
        tag2 = get_tag(43)
        # DANGER_GOODS 是否经营危险品
        tag3 = get_tag(45)
        # COLD_FOOD 是否从事冷链食品
        tag4 = get_tag(29)
        # EST_YEARS 成立年限
        tag5 = get_tag(39)
        # REG_CAPITAL 注册资本
        tag6 = get_tag(9)
        # I_E_TYPE 进出口方式
        tag7 = get_tag(28)
        # CORP_TYPE 企业类型
        tag8 = get_tag(10)
        # INDUSTRY 所属行业
        tag9 = get_tag(12)
        # TAX_RATE 纳税信用等级
        tag10 = get_tag(21)
        # CUS_RATE 海关信用等级
        tag11 = get_tag(22)
        # EXC_RATE 外汇管理信用等级
        tag12 = get_tag(20)

        #### add crop_info :TRADE_CODE label_code and labename

        def get_add_trade_info():
            sql = """ SELECT DISTINCT CREDIT_CODE ORG_CODE,TRADE_CODE,CUSTOM_ID,LABEL_CODE FROM {} WHERE CREDIT_CODE IS NOT NULL
                      AND STATUS = '5' AND SOURCE = '2' """.format('N_EPZ_CUS.TRADE')

            Ttrade_Data = Read_Oracle().read_oracle(sql=sql, database='dbods')

            sql_code = """ SELECT DISTINCT LABEL_CODE,LABEL_NAME FROM {} WHERE LABEL_CODE IS NOT NULL""".format('N_EPZ_CUS.TRADE_LABEL')
            Ttrade_Data_Lbael = Read_Oracle().read_oracle(sql=sql_code, database='dbods')
            Ttrade_Data_Lbael = dict(zip(Ttrade_Data_Lbael['LABEL_CODE'],Ttrade_Data_Lbael['LABEL_NAME']))
            Ttrade_Data['LABEL_NAME'] =Ttrade_Data['LABEL_CODE'].apply(lambda x: ",".join(map(str,[Ttrade_Data_Lbael.get(v) for v in (x.split(','))])))
            return Ttrade_Data



        def corp_info_main():
            '''
            整合各类数据之后的汇总函数
            :return:
            '''
            dw_crop_info_df = dw_corp_info()
            cbrs_df = ods_cbrs()
            # 关联DW与参保人数关联到企业名称
            cbrs_df.drop_duplicates(['DWMC'], keep='last', inplace=True)
            dw_crop_info_df_cb = pd.merge(dw_crop_info_df, cbrs_df, left_on='CORP_NAME', right_on='DWMC',
                                          how='left')
            dw_crop_info_df_cb.drop(['DWMC'], axis=1, inplace=True)

            # 获取企业的行业代码
            Industry_code_df = dw_dim_industry_code()
            Industry_code_df = Industry_code_df[['HY_DM', 'HYMC']].drop_duplicates()
            dw_crop_info_df_cb['INDUSTRY_CODE'] = dw_crop_info_df_cb['INDUSTRY_CODE'].map(lambda x: str(x)[1:])
            dw_crop_info_df_cb_indco = pd.merge(dw_crop_info_df_cb, Industry_code_df, left_on='INDUSTRY_CODE',
                                                right_on='HY_DM', how='left')
            dw_crop_info_df_cb_indco.drop(['HY_DM', 'INDUSTRY_CODE'], axis=1, inplace=True)

            dw_crop_info_df_cb_indco = dw_crop_info_df_cb_indco.rename(
                columns={'TO_CHAR(BUSINESS_SCOPE)': 'BUSINESS_SCOPE'})
            # 00010000 企业   00010100 公司     00010200 分公司
            dw_crop_info_df_cb_indco['CORP_TYPE'] = dw_crop_info_df_cb_indco['CORP_TYPE'].map(
                {'00010100': '公司', '00010200': '分公司'})
            # RECEIVING_ORGAN 受理机关
            del dw_crop_info_df_cb_indco['RECEIVING_ORGAN']

            # 纳税等级
            tax_level = ods_tax_level()
            dw_crop_info_df_cb_indco_tax = pd.merge(dw_crop_info_df_cb_indco, tax_level, left_on='UNI_SC_ID',
                                                    right_on='SHXYDM', how='left')
            dw_crop_info_df_cb_indco_tax.drop(['NSRMC', 'SHXYDM'], axis=1, inplace=True)
            # 海关信用等级
            cus_level = ods_cos_level()
            res = pd.merge(dw_crop_info_df_cb_indco_tax, cus_level[['UNI_SC_ID', 'MTYPE']], on='UNI_SC_ID',
                           how='left')
            return res



        corp_info = corp_info_main()
        corp_info['I_E_METHOD'] = tag1
        corp_info['CUS_QUA'] = tag2
        corp_info['DANGER_GOODS'] = tag3
        corp_info['COLD_FOOD'] = tag4
        corp_info['EST_YEARS'] = tag5
        corp_info['REG_CAPITAL'] = tag6
        corp_info['I_E_TYPE'] = tag7
        corp_info['CORP_TYPE'] = tag8
        corp_info['INDUSTRY'] = tag9
        corp_info['TAX_RATE'] = tag10
        corp_info['CUS_RATE'] = tag11
        corp_info['EXC_RATE'] = tag12
        corp_info['FIRSTINSERTDT'] = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        corp_info['FIRSTINSERTDT'] = pd.to_datetime(corp_info['FIRSTINSERTDT'], format='%Y-%m-%d %H:%M:%S')
        final = corp_info.reset_index().rename(columns={'index': 'ID'})
        final.rename(columns={'CORP_NAME': 'ORG_NAME', 'UNI_SC_ID': 'ORG_CODE'}, inplace=True)
        # TODO 这里需要调取高德信息获取经纬度数据，或者手动输入经纬度
        # final['LON'] = '121.859346'
        # final['LAT'] = '31.087247'
        # final['LON'] = i['LONGITUDE']
        # final['LAT'] = i['LATITUDE']
        sql = ''' select longitude, latitude from DIM.DIM_FTZ_CORP where iscurrent = 1 and uni_sc_id = '{}' and zmxpq_flag = 1'''.format(org_code)
        df_axis = Read_Oracle().read_oracle(sql=sql, database='dbdw')
        final['LON'] = df_axis['LONGITUDE']
        final['LAT'] = df_axis['LATITUDE']

        ###add trand_code lable_name
        Trade_Lab_inf = get_add_trade_info()
        final = pd.merge(final, Trade_Lab_inf, on='ORG_CODE', how='left', suffixes=('_x', '_y'))

        final = final[
            ['ID', 'ORG_CODE', 'ORG_NAME', 'TRADE_CODE','CUSTOM_ID','I_E_METHOD', 'CUS_QUA', 'DANGER_GOODS', 'COLD_FOOD', 'EST_YEARS',
             'REG_CAPITAL', 'I_E_TYPE', 'CORP_TYPE', 'INDUSTRY', 'TAX_RATE', 'CUS_RATE', 'EXC_RATE', 'ADDRESS',
             'LON', 'LAT', 'REG_NO', 'CORP_COUNTRY', 'SUB_OBJ', 'HYMC', 'BUSINESS_SCOPE', 'BUSINESS_ADDRESS',
             'AREA_CODE', 'ZIP', 'TELEPHONE', 'ESTABLISH_DATE', 'CURRENCY', 'PERSON_NAME', 'PERSON_CERT_TYPE',
             'PERSON_CERT_CODE', 'CORP_STATUS', 'PERSON_EMAIL', 'PERSON_LANDLINE_TEL', 'CHANGE_DATE', 'CHANGE_ITEM',
             'CONTACT_MOBILE', 'CONTACT_EMAIL', 'CONTACT_CER_TYPE', 'CONTACT_CER_NO', 'DWCBRS', 'JNJE', 'SFQJ',
             'CBNY', 'ZZZGPJRS', 'PDDJ', 'MTYPE','LABEL_CODE','LABEL_NAME']]
        final['CHECK_TIME'] = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        final['CHECK_TIME'] = pd.to_datetime(final['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        final.fillna('', inplace=True)
        Write_Oracle().write_oracle('BD_RISK_CORP_INFO_BASIC', final, org_code=org_code, alarm=None)

    def run_model_dp4(self):
        try:
            self.model_dp4()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    # child_task_id = sys.argv[1]
    child_task_id = 'childtaskiddp4001'
    org_code, param_json, base_time = read_log_table(child_task_id)
    DisplayDp4(org_code=org_code, param_json=param_json, base_time=base_time,
               child_task_id=child_task_id).run_model_dp4()
