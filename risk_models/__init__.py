"""
此处存放各类module和接口的导入，以及引入表的映射
便于module的管理
"""
import sys
import os
from os import path

# 部署的路径
sys.path.append('/root/bdrisk/risk_model')
# 堡垒机的路径
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
# 这是log文件的存放路径
root_path = path.dirname(path.dirname(path.dirname(os.getcwd())))

# 引入各个module
from risk_models.config.read_config.read_func import Read_Oracle
from risk_models.config.write_config.write_func import Write_Oracle
from risk_models.config.write_config.write_func_alarm import Write_Oracle_Alarm
from risk_models.config.log_config.read_log import read_log_table
from risk_models.config.auth_config.ezpass_table import TableList
from risk_models.config.log_config.log_func import Risk_logger
from risk_models.config.write_config.write_track import Write_Track
from risk_models.config.param_config import params_global

# 导入第三方库
import datetime
from datetime import datetime as dt_method
import json
from loguru import logger
import pandas as pd
pd.set_option('display.max_columns', None)
import sys
from os import path
import os
import numpy as np
import re

# 实例化各个表的对象名称
# 企业ERP数据表格
_name_BD_RISK_MODEL_LOG = TableList.BD_RISK_MODEL_LOG.value
_name_OPENING_INVENTORY = TableList.OPENING_INVENTORY.value
_name_OPENING_INVENTORY_DETAIL = TableList.OPENING_INVENTORY_DETAIL.value
_name_EMS_STOCK_BILL = TableList.EMS_STOCK_BILL.value
_name_EMS_FINANCE_INFO = TableList.EMS_FINANCE_INFO.value
_name_EMS_MANUFACTURE_TOTAL = TableList.EMS_MANUFACTURE_TOTAL.value
_name_EMS_WORK_INPUT = TableList.EMS_WORK_INPUT.value
_name_EMS_WORK_OUTPUT = TableList.EMS_WORK_OUTPUT.value
_name_EMS_TIMECOST_INFO = TableList.EMS_TIMECOST_INFO.value
_name_BD_RISK_WAREHOUSE_ORDER_HEAD = TableList.BD_RISK_WAREHOUSE_ORDER_HEAD.value
_name_BD_RISK_WAREHOUSE_ORDER = TableList.BD_RISK_WAREHOUSE_ORDER.value
_name_WAREHOUSE_STOCK_BILL = TableList.WAREHOUSE_STOCK_BILL.value

# 模型明细与结果表
# STOCK
# ST1
_name_BD_RISK_RESULT_STOCK_ST1 = TableList.BD_RISK_RESULT_STOCK_ST1.value
_name_BD_RISK_DETAIL_STOCK_ST1 = TableList.BD_RISK_DETAIL_STOCK_ST1.value
# ST2
_name_BD_RISK_RESULT_STOCK_ST2 = TableList.BD_RISK_RESULT_STOCK_ST2.value
_name_BD_RISK_DETAIL_STOCK_ST2 = TableList.BD_RISK_DETAIL_STOCK_ST2.value
# ST3
_name_BD_RISK_RESULT_STOCK_ST3 = TableList.BD_RISK_RESULT_STOCK_ST3.value
_name_BD_RISK_DETAIL_STOCK_ST3 = TableList.BD_RISK_DETAIL_STOCK_ST3.value
# ST4
_name_BD_RISK_RESULT_STOCK_ST4 = TableList.BD_RISK_RESULT_STOCK_ST4.value
_name_BD_RISK_DETAIL_STOCK_ST4 = TableList.BD_RISK_DETAIL_STOCK_ST4.value
# ST5
_name_BD_RISK_RESULT_STOCK_ST5 = TableList.BD_RISK_RESULT_STOCK_ST5.value
_name_BD_RISK_DETAIL_STOCK_ST5 = TableList.BD_RISK_DETAIL_STOCK_ST5.value

# FINANCE
# FN1
_name_BD_RISK_RESULT_FINANCE_FN1 = TableList.BD_RISK_RESULT_FINANCE_FN1.value
_name_BD_RISK_DETAIL_FINANCE_FN1 = TableList.BD_RISK_DETAIL_FINANCE_FN1.value
# FN2
_name_BD_RISK_RESULT_FINANCE_FN2 = TableList.BD_RISK_RESULT_FINANCE_FN2.value
_name_BD_RISK_DETAIL_FINANCE_FN2 = TableList.BD_RISK_DETAIL_FINANCE_FN2.value

# PRODUCTION
# PD1
_name_BD_RISK_RESULT_PRODUCTION_PD1 = TableList.BD_RISK_RESULT_PRODUCTION_PD1.value
_name_BD_RISK_DETAIL_PRODUCTION_PD1 = TableList.BD_RISK_DETAIL_PRODUCTION_PD1.value
# PD2
_name_BD_RISK_RESULT_PRODUCTION_PD2 = TableList.BD_RISK_RESULT_PRODUCTION_PD2.value
_name_BD_RISK_DETAIL_PRODUCTION_PD2 = TableList.BD_RISK_DETAIL_PRODUCTION_PD2.value
# PD3
_name_BD_RISK_RESULT_PRODUCTION_PD3 = TableList.BD_RISK_RESULT_PRODUCTION_PD3.value
_name_BD_RISK_DETAIL_PRODUCTION_PD3 = TableList.BD_RISK_DETAIL_PRODUCTION_PD3.value
# PD4
_name_BD_RISK_RESULT_PRODUCTION_PD4 = TableList.BD_RISK_RESULT_PRODUCTION_PD4.value
_name_BD_RISK_DETAIL_PRODUCTION_PD4 = TableList.BD_RISK_DETAIL_PRODUCTION_PD4.value

# WAREHOUSE
# WH1
# WH2
_name_BD_RISK_DETAIL_WAREHOUSE_WH2 = TableList.BD_RISK_DETAIL_WAREHOUSE_WH2.value
_name_BD_RISK_RESULT_WAREHOUSE_WH2 = TableList.BD_RISK_DETAIL_PRODUCTION_PD2.value

# SUPPLYCHAIN
# SC1
_name_FT_I_DTL_SEA_PRE_RECORDED = TableList.FT_I_DTL_SEA_PRE_RECORDED.value
_name_FT_I_DTL_SEA_CONTAINER = TableList.FT_I_DTL_SEA_CONTAINER.value
_name_FT_I_DTL_COARRI_CTNR = TableList.FT_I_DTL_COARRI_CTNR.value
_name_DIM_OPERATOR = TableList.DIM_OPERATOR.value
_name_DIM_FTZ_CORP = TableList.DIM_FTZ_CORP.value
_name_FT_E_DTL_SEA_PRE_RECORDED = TableList.FT_E_DTL_SEA_PRE_RECORDED.value
_name_FT_E_DTL_SEA_CONTAINER = TableList.FT_E_DTL_SEA_CONTAINER.value
_name_FT_E_DTL_COARRI_CTNR = TableList.FT_E_DTL_COARRI_CTNR.value
_name_MX_BVD = TableList.MX_BVD.value
_name_BD_RISK_DETAIL_SUPPLYCHAIN_SC1 = TableList.BD_RISK_DETAIL_SUPPLYCHAIN_SC1.value
_name_BD_RISK_RESULT_SUPPLYCHAIN_SC1 = TableList.BD_RISK_RESULT_SUPPLYCHAIN_SC1.value

# DISPLAY
# DP1
_name_FT_I_DTL_OTR_PRE_RECORDED = TableList.FT_I_DTL_OTR_PRE_RECORDED.value
_name_FT_I_DTL_SEA_LIST = TableList.FT_I_DTL_SEA_LIST.value
_name_FT_I_DTL_OTR_LIST = TableList.FT_I_DTL_OTR_LIST.value
_name_FT_E_DTL_OTR_PRE_RECORDED = TableList.FT_E_DTL_OTR_PRE_RECORDED.value
_name_FT_E_DTL_SEA_LIST = TableList.FT_E_DTL_SEA_LIST.value
_name_FT_E_DTL_OTR_LIST = TableList.FT_E_DTL_OTR_LIST.value
_name_DIM_TRADER = TableList.DIM_TRADER.value

# DP4
_name_BD_RISK_CORP_INFO_BASIC = TableList.BD_RISK_CORP_INFO_BASIC.value


# CREDIT
# CR3
_name_CORP_INFO = TableList.CORP_INFO.value
_name_ZWY_DWCBQK_XXB = TableList.ZWY_DWCBQK_XXB.value
_name_WATER_RATE_USAGE = TableList.WATER_RATE_USAGE.value
_name_ELECTRIC_CHARGE_USAGE = TableList.ELECTRIC_CHARGE_USAGE.value
_name_NATURAL_GAS_USAGE = TableList.NATURAL_GAS_USAGE.value
_name_FT_CUS_DWS_TRADE = TableList.FT_CUS_DWS_TRADE.value
_name_BD_RISK_DETAIL_CREDIT_CR3 = TableList.BD_RISK_DETAIL_CREDIT_CR3.value
_name_BD_RISK_RESULT_CREDIT_CR3 = TableList.BD_RISK_RESULT_CREDIT_CR3.value

# TRACK
# TR1
_name_BD_RISK_RESULT_TRACK_TR1 = TableList.BD_RISK_RESULT_TRACK_TR1.value
_name_BD_RISK_TRACK_INFO = TableList.BD_RISK_TRACK_INFO.value

# TRADE
# TD2
_name_FT_STA_GOODSOWNER_MAIN_CLASS = TableList.FT_STA_GOODSOWNER_MAIN_CLASS.value
_name_DW_CORP_CUSDEC = TableList.DW_CORP_CUSDEC.value
_name_FT_CUS_DWS_ENTRY = TableList.FT_CUS_DWS_ENTRY.value
_name_BD_RISK_RESULT_TRADE_TD2 = TableList.BD_RISK_RESULT_TRADE_TD2.value



# TD1
_name_CUSTOMS_CREDIT = TableList.CUSTOMS_CREDIT.value
_name_BD_RISK_RESULT_TRADE_TD1 = TableList.BD_RISK_RESULT_TRADE_TD1.value