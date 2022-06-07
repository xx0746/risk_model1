import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk-model\risk_model\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
    print(path.dirname(path.dirname(os.getcwd())))
from risk_models import *
from dateutil.relativedelta import *
import os
import cx_Oracle
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

RES_TBL = 'BD_RISK_CROSS_TRADE_RESULT_CREDIT_SCORE_TBL_CLEANV2'
USER_NAME = "DW_CUS_RC"
PASSWORD = "easipass"
ADDRESS = "192.168.130.225:1521/pdbcusdev"

class CREDIT_MODEL(object):
    def __init__(self, org_code, base_time, child_task_id):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.base_time = base_time

    def clean_layer(self):
        one_delta_years = relativedelta(years=1)
        two_delta_years = relativedelta(years=2)
        three_delta_years = relativedelta(years=3)
        five_delta_years = relativedelta(years=5)
        one_delta_months = relativedelta(months=1)
        time_now = datetime.datetime.now()
        one_years_before = time_now - one_delta_years
        two_years_before = time_now - two_delta_years
        three_years_before = time_now - three_delta_years
        five_years_before = time_now - five_delta_years
        one_month_before = time_now - one_delta_months
        last_res_df = pd.DataFrame()

        # 获取需要评分信用的企业
        sql_text1 = f'''
            select CREDIT_CODE, COMPANY_NAME, BUSINESS_TIME AS D_DATE
            from DW_CUS_RC.COMPANY_CREDIT T1
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' AND T1.ISCURRENT = 1
                     '''
        last_res_df = Read_Oracle().read_oracle(sql=sql_text1, database='dbalarm')
        if last_res_df is None and len(last_res_df) == 0:
            return


        # 获取企业海关评级
        sql_text1 = f'''
            select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, T2.CO_CLASS as CUSTOM
            from DW_CUS_RC.COMPANY_CREDIT T1
            LEFT JOIN DW_CUS_RC.COMPANY_REL T2
            ON T1.CREDIT_CODE = T2.LICENSE_ID
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}'
            AND T2.ISCURRENT = 1
                            '''
        custom_df = Read_Oracle().read_oracle(sql=sql_text1, database='dbalarm')
        if custom_df is not None and len(custom_df) != 0:
            last_res_df = pd.merge(last_res_df, custom_df, on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"], how='left')
            print(last_res_df)
        zero_res_df = pd.DataFrame()

        # 获取5年内刑事
        sql_text1_2 = f'''
                select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS CRIMINAL_CUSTOM_IN_FIVE
                from DW_CUS_RC.COMPANY_CREDIT T1
                LEFT JOIN DW_CUS_RC.T_JUDGEMENT T2
                ON T1.CREDIT_CODE = T2.COMPANY_ID
                WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' and to_char(T2.SUBMIT_DATE, 'yyyymmdd') > '{five_years_before.strftime("%Y%m%d")}' AND T2.ISCURRENT = 1
                GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                              '''
        criminal_custom_in_five_df = Read_Oracle().read_oracle(sql=sql_text1_2, database='dbalarm')
        print(criminal_custom_in_five_df)
        if criminal_custom_in_five_df is not None and len(criminal_custom_in_five_df) != 0:
            last_res_df = pd.merge(last_res_df, criminal_custom_in_five_df,
                                   on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"], how='left')
            print(last_res_df)
            zero_res_df = last_res_df[last_res_df['CRIMINAL_CUSTOM_IN_FIVE'] > 0]
            last_res_df = last_res_df[pd.isna(last_res_df['CRIMINAL_CUSTOM_IN_FIVE'])]
            last_res_df = last_res_df.fillna(0)

        # 获取企业一年前税务信用等级
        sql_text2 = f'''
                select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, T2.TAX_LEVEL as TAX_CREDIT
                from DW_CUS_RC.COMPANY_CREDIT T1
                LEFT JOIN DW_CUS_RC.T_TAXCREDIT T2
                ON T1.CREDIT_CODE = T2.COMPANY_ID
                WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' AND T2.YEAR = '{one_years_before.strftime("%Y")}' AND T2.ISCURRENT = 1
                            '''
        tax_credit_df = Read_Oracle().read_oracle(sql=sql_text2, database='dbalarm')
        if tax_credit_df is not None and len(tax_credit_df) != 0:
            last_res_df = pd.merge(last_res_df, tax_credit_df, how="left",
                                   on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"])
            zero_res_df['TAX_CREDIT'] = 'N/A'
            last_res_df = last_res_df.fillna('N/A')

        # 获取企业总共行政处罚次数
        sql_text3_1 = f'''
                select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.company_id) AS ADMINISTRATION
                from DW_CUS_RC.COMPANY_CREDIT T1
                LEFT JOIN DW_CUS_RC.T_ECI_PENALTY T2
                ON T1.CREDIT_CODE = T2.COMPANY_ID
                WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}'
                AND T2.ISCURRENT = 1
                GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                            '''
        adminstration_total_df = Read_Oracle().read_oracle(sql=sql_text3_1, database='dbalarm')
        if adminstration_total_df is not None and len(adminstration_total_df) != 0:
            last_res_df = pd.merge(last_res_df, adminstration_total_df,
                                   on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"], how="left")
            zero_res_df['ADMINISTRATION'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业3年内行政处罚次数
        sql_text3_2 = f'''
            select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS ADMINISTRATION_3_YEARS
            from DW_CUS_RC.COMPANY_CREDIT T1
            LEFT JOIN DW_CUS_RC.T_ECI_PENALTY T2
            ON T1.CREDIT_CODE = T2.COMPANY_ID
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' 
            and to_char(T2.PENALTY_DATE, 'yyyymmdd') >= '{three_years_before.strftime("%Y%m%d")}'
            AND T2.ISCURRENT = 1
            GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                                '''
        adminstration_three_years_df = Read_Oracle().read_oracle(sql=sql_text3_2, database='dbalarm')
        if adminstration_three_years_df is not None and len(adminstration_three_years_df) != 0:
            last_res_df = pd.merge(last_res_df, adminstration_three_years_df,
                                   on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"], how="left")
            zero_res_df['ADMINISTRATION_3_YEARS'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业5年外海关刑事处罚次数
        sql_text4_1 = f'''
                select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS CRIMINAL_CUSTOM
                from DW_CUS_RC.COMPANY_CREDIT T1
                LEFT JOIN DW_CUS_RC.T_JUDGEMENT T2
                ON T1.CREDIT_CODE = T2.COMPANY_ID
                WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' 
                and to_char(T2.SUBMIT_DATE, 'yyyymmdd') < '{five_years_before.strftime("%Y%m%d")}' 
                and case_reason_type = 'hg'
                AND T2.ISCURRENT = 1
                GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                              '''
        criminal_custom_df = Read_Oracle().read_oracle(sql=sql_text4_1, database='dbalarm')
        if criminal_custom_df is not None and len(criminal_custom_df) != 0:
            last_res_df = pd.merge(last_res_df, criminal_custom_df, on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"],
                                   how="left")
            zero_res_df['CRIMINAL_CUSTOM'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业5年外非海关刑事处罚次数
        sql_text4_2 = f'''
                select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS CRIMINAL_NON_CUSTOM
                from DW_CUS_RC.COMPANY_CREDIT T1
                LEFT JOIN DW_CUS_RC.T_JUDGEMENT T2
                ON T1.CREDIT_CODE = T2.COMPANY_ID
                WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' 
                and to_char(T2.SUBMIT_DATE, 'yyyymmdd') < '{five_years_before.strftime("%Y%m%d")}' 
                and case_reason_type = 'xs'
                AND T2.ISCURRENT = 1
                GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                                '''
        criminal_non_custom_df = Read_Oracle().read_oracle(sql=sql_text4_2, database='dbalarm')
        if criminal_non_custom_df is not None and len(criminal_non_custom_df) != 0:
            last_res_df = pd.merge(last_res_df, criminal_non_custom_df,
                                   on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"], how="left")
            zero_res_df['CRIMINAL_NON_CUSTOM'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业前第一年失信次数
        sql_text5_1 = f'''
                select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS LOST_CREDIT_1ST
                from DW_CUS_RC.COMPANY_CREDIT T1
                LEFT JOIN DW_CUS_RC.T_SHIXIN T2
                ON T1.CREDIT_CODE = T2.COMPANY_ID
                WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' 
                and to_char(T2.LIAN_DATE, 'yyyymmdd') >= '{one_years_before.strftime("%Y%m%d")}'
                AND T2.ISCURRENT = 1
                GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                                '''
        lost_credit_one_df = Read_Oracle().read_oracle(sql=sql_text5_1, database='dbalarm')
        if lost_credit_one_df is not None and len(lost_credit_one_df) != 0:
            last_res_df = pd.merge(last_res_df, lost_credit_one_df, on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"],
                                   how="left")
            zero_res_df['LOST_CREDIT_1ST'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业前第二年失信次数
        sql_text5_2 = f'''
                select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS LOST_CREDIT_2ND
                from DW_CUS_RC.COMPANY_CREDIT T1
                LEFT JOIN DW_CUS_RC.T_SHIXIN T2
                ON T1.CREDIT_CODE = T2.COMPANY_ID
                WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' 
                and to_char(T2.LIAN_DATE, 'yyyymmdd') < '{one_years_before.strftime("%Y%m%d")}' 
                and to_char(T2.LIAN_DATE, 'yyyymmdd') >= '{two_years_before.strftime("%Y%m%d")}'
                AND T2.ISCURRENT = 1
                GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                                '''
        lost_credit_two_df = Read_Oracle().read_oracle(sql=sql_text5_2, database='dbalarm')
        if lost_credit_two_df is not None and len(lost_credit_two_df) != 0:
            last_res_df = pd.merge(last_res_df, lost_credit_two_df, on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"],
                                   how="left")
            zero_res_df['LOST_CREDIT_2ND'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业前第三年失信次数
        sql_text5_3 = f'''
            select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS LOST_CREDIT_3RD
            from DW_CUS_RC.COMPANY_CREDIT T1
            LEFT JOIN DW_CUS_RC.T_SHIXIN T2
            ON T1.CREDIT_CODE = T2.COMPANY_ID
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' 
            and to_char(T2.LIAN_DATE, 'yyyymmdd') < '{two_years_before.strftime("%Y%m%d")}' 
            and to_char(T2.LIAN_DATE, 'yyyymmdd') >= '{three_years_before.strftime("%Y%m%d")}'
            AND T2.ISCURRENT = 1
            GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                            '''
        lost_credit_three_df = Read_Oracle().read_oracle(sql=sql_text5_3, database='dbalarm')
        if lost_credit_three_df is not None and len(lost_credit_three_df) != 0:
            last_res_df = pd.merge(last_res_df, lost_credit_three_df,
                                   on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"], how="left")
            zero_res_df['LOST_CREDIT_3RD'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业总共失信次数
        sql_text5_4_total = f'''
            select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS LOST_CREDIT_TOTAL
            from DW_CUS_RC.COMPANY_CREDIT T1
            LEFT JOIN DW_CUS_RC.T_SHIXIN T2
            ON T1.CREDIT_CODE = T2.COMPANY_ID
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}'
            AND T2.ISCURRENT = 1
            GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                                '''
        lost_credit_total_df = Read_Oracle().read_oracle(sql=sql_text5_4_total, database='dbalarm')
        if lost_credit_total_df is not None and len(lost_credit_total_df) != 0:
            last_res_df = pd.merge(last_res_df, lost_credit_total_df,
                                   on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"], how="left")
            zero_res_df['LOST_CREDIT_TOTAL'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业前第一年被执行次数
        sql_text6_1 = f'''
            select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS EXCUTED_1ST
            from DW_CUS_RC.COMPANY_CREDIT T1
            LEFT JOIN DW_CUS_RC.T_ZHIXING T2
            ON T1.CREDIT_CODE = T2.COMPANY_ID
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' 
            and to_char(T2.LIAN_DATE, 'yyyymmdd') >= '{one_years_before.strftime("%Y%m%d")}'
            AND T2.ISCURRENT = 1
            GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                                '''
        excuted_one_df = Read_Oracle().read_oracle(sql=sql_text6_1, database='dbalarm')
        if excuted_one_df is not None and len(excuted_one_df) != 0:
            last_res_df = pd.merge(last_res_df, excuted_one_df, on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"],
                                   how="left")
            zero_res_df['EXCUTED_1ST'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业前第二年被执行次数
        sql_text6_2 = f'''
            select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS EXCUTED_2ND
            from DW_CUS_RC.COMPANY_CREDIT T1
            LEFT JOIN DW_CUS_RC.T_ZHIXING T2
            ON T1.CREDIT_CODE = T2.COMPANY_ID
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' 
            and to_char(T2.LIAN_DATE, 'yyyymmdd') < '{one_years_before.strftime("%Y%m%d")}' 
            and to_char(T2.LIAN_DATE, 'yyyymmdd') >= '{two_years_before.strftime("%Y%m%d")}'
            AND T2.ISCURRENT = 1
            GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                                '''
        excuted_two_df = Read_Oracle().read_oracle(sql=sql_text6_2, database='dbalarm')
        if excuted_two_df is not None and len(excuted_two_df) != 0:
            last_res_df = pd.merge(last_res_df, excuted_two_df, on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"],
                                   how="left")
            zero_res_df['EXCUTED_2ND'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业前第三年被执行次数
        sql_text6_3 = f'''
            select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS EXCUTED_3RD
            from DW_CUS_RC.COMPANY_CREDIT T1
            LEFT JOIN DW_CUS_RC.T_ZHIXING T2
            ON T1.CREDIT_CODE = T2.COMPANY_ID
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' 
            and to_char(T2.LIAN_DATE, 'yyyymmdd') < '{two_years_before.strftime("%Y%m%d")}' 
            and to_char(T2.LIAN_DATE, 'yyyymmdd') >= '{three_years_before.strftime("%Y%m%d")}'
            AND T2.ISCURRENT = 1
            GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                            '''
        excuted_three_df = Read_Oracle().read_oracle(sql=sql_text6_3, database='dbalarm')
        if excuted_three_df is not None and len(excuted_three_df) != 0:
            last_res_df = pd.merge(last_res_df, excuted_three_df, on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"],
                                   how="left")
            zero_res_df['EXCUTED_3RD'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业总共被执行次数
        sql_text6_4_total = f'''
                select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS EXCUTED_TOTAL
                from DW_CUS_RC.COMPANY_CREDIT T1
                LEFT JOIN DW_CUS_RC.T_ZHIXING T2
                ON T1.CREDIT_CODE = T2.COMPANY_ID
                WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}'
                AND T2.ISCURRENT = 1
                GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                            '''
        excuted_total_df = Read_Oracle().read_oracle(sql=sql_text6_4_total, database='dbalarm')
        if excuted_total_df is not None and len(excuted_total_df) != 0:
            last_res_df = pd.merge(last_res_df, excuted_total_df, on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"], how="left")
            zero_res_df['EXCUTED_TOTAL'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业总共非正常经营次数
        sql_text7_1 = f'''
            select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS NOT_OPERATION
            from DW_CUS_RC.COMPANY_CREDIT T1
            LEFT JOIN DW_CUS_RC.T_ECI_OPERATEEXCEPTION T2
            ON T1.CREDIT_CODE = T2.COMPANY_ID
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}'
            AND T2.ISCURRENT = 1
            GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                        '''
        non_operation_df = Read_Oracle().read_oracle(sql=sql_text7_1, database='dbalarm')
        if non_operation_df is not None and len(non_operation_df) != 0:
            last_res_df = pd.merge(last_res_df, non_operation_df, on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"], how="left")
            zero_res_df['NOT_OPERATION'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业三年内非正常经营次数
        sql_text7_2 = f'''
            select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, COUNT(T2.COMPANY_ID) AS NOT_OPERATION_3_YEARS
            from DW_CUS_RC.COMPANY_CREDIT T1
            LEFT JOIN DW_CUS_RC.T_ECI_OPERATEEXCEPTION T2
            ON T1.CREDIT_CODE = T2.COMPANY_ID
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}' 
            and to_char(REMOVE_DATE, 'yyyymmdd') >= '{three_years_before.strftime("%Y%m%d")}'
            AND T2.ISCURRENT = 1
            GROUP BY T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME
                                '''
        non_operation_three_years_df = Read_Oracle().read_oracle(sql=sql_text7_2, database='dbalarm')
        if non_operation_three_years_df is not None and len(non_operation_three_years_df) != 0:
            last_res_df = pd.merge(last_res_df, non_operation_three_years_df,
                                   on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"], how="left")
            zero_res_df['NOT_OPERATION_3_YEARS'] = 0
            last_res_df = last_res_df.fillna(0)

        # 获取企业社保人数
        sql_text8 = f'''
            select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, T2.YL_INS as SOCIAL_SECURE
            from DW_CUS_RC.COMPANY_CREDIT T1
            LEFT JOIN DW_CUS_RC.T_ANNUAL_SOCIALSECURITY T2
            ON T1.CREDIT_CODE = T2.COMPANY_ID
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}'
            AND T2.ISCURRENT = 1
                            '''
        social_secure_df = Read_Oracle().read_oracle(sql=sql_text8, database='dbalarm')
        if social_secure_df is not None and len(social_secure_df) != 0:
            last_res_df = pd.merge(last_res_df, social_secure_df, on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"],
                                   how="left")
            zero_res_df['SOCIAL_SECURE'] = 0
            last_res_df['SOCIAL_SECURE'] = last_res_df['SOCIAL_SECURE'].map(lambda x: 0 if x == 'N/A' else x)
            last_res_df = last_res_df.fillna(0)

        # 获取企业工商成立时间
        sql_text9 = f'''
            select T1.CREDIT_CODE, T1.COMPANY_NAME, T1.BUSINESS_TIME AS D_DATE, T2.START_DATE as START_FROM_NOW
            from DW_CUS_RC.COMPANY_CREDIT T1
            LEFT JOIN DW_CUS_RC.T_ECI_COMPANY T2
            ON T1.CREDIT_CODE = T2.COMPANY_ID
            WHERE to_char(T1.BUSINESS_TIME, 'yyyymmdd') >= '{one_month_before.strftime("%Y%m%d")}'
            AND T2.ISCURRENT = 1
                        '''
        business_df = Read_Oracle().read_oracle(sql=sql_text9, database='dbalarm')
        if business_df is not None and len(business_df) != 0:
            business_df["START_FROM_NOW"] = business_df["START_FROM_NOW"].map(
                lambda x: 0 if x == 'N/A' else (time_now - x).days / 365)
            last_res_df = pd.merge(last_res_df, business_df, on=["CREDIT_CODE", "COMPANY_NAME", "D_DATE"], how="left")
            zero_res_df['START_FROM_NOW'] = 0
            last_res_df = last_res_df.fillna(0)

        if last_res_df is not None and len(last_res_df) != 0:
            last_res_df['ID'] = 0
            last_res_df['CAL_TYPE'] = 1
            last_res_df['ISCURRENT'] = 1
            last_res_df['D_DATE'] = last_res_df['D_DATE'].map(lambda x: x.strftime("%Y%m%d"))
            print(last_res_df)

        if zero_res_df is not None and len(zero_res_df) != 0:
            zero_res_df['ID'] = 0
            zero_res_df['CAL_TYPE'] = 0
            zero_res_df['ISCURRENT'] = 1
            zero_res_df['D_DATE'] = zero_res_df['D_DATE'].map(lambda x: x.strftime("%Y%m%d"))
            print(zero_res_df)
            print("------------------------------")

        # 写入数据库
        self.change_all_iscurrent()
        if last_res_df is not None and len(last_res_df) != 0:
            Write_Oracle_Alarm().write_oracle(RES_TBL, last_res_df, org_code=None, alarm=None, iscurrent=False)

        if zero_res_df is not None and len(zero_res_df) != 0:
            Write_Oracle_Alarm().write_oracle(RES_TBL, zero_res_df, org_code=None, alarm=None, iscurrent=False)

    def change_all_iscurrent(self):
        db = cx_Oracle.connect(USER_NAME, PASSWORD, ADDRESS)
        cursor = db.cursor()
        sql_text = f'''
            update {RES_TBL} set ISCURRENT = 0  where iscurrent = 1"
                    '''
        cursor.execute(sql_text)
        db.commit()
        cursor.close()
        db.close()

    def run_clean_layer(self):
        try:
            self.clean_layer()
            exec_status = 1
        except Exception as e:
            errorStr = f'''{e}'''
            errorStr = errorStr.replace("'", " ")
            errorStr = errorStr[-3900:]
            logger.exception(errorStr)
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    if params_global.is_test:
        child_task_id = 'CREDIT_MODEL_CLEAN'
    else:
        child_task_id = sys.argv[1]
    # org_code, param_json, base_time = read_log_table(child_task_id)
    CREDIT_MODEL(None, None, child_task_id).run_clean_layer()