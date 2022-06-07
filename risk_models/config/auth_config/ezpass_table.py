from enum import Enum, unique
from risk_models.config.param_config import params_global

@unique
class TableList(Enum):
    """
    here to save all read table for Easipass Database table reference
    all table changes must made here
    """
    if params_global.is_test:
        # 测试环境,请注意切换！！！！！！！
        # oracle-ods
        # 日志表
        BD_RISK_MODEL_LOG = 'BD_RISK_MODEL_LOG'
        #大模块分数表
        BD_RISK_CORP_SCORE_DISPLAY = 'BD_RISK_CORP_SCORE_DISPLAY'
        # 预警表
        BD_RISK_ALARM_ITEM ='BD_RISK_ALARM_ITEM'

        # 企业信息列表
        ft_gov_dtl_corp_info = 'dw_lgxc_basic.ft_gov_dtl_corp_info'
        # 涉诉信息
        MX_SHESU = 'MX_SHESU'
        # 失信信息
        MX_SHIXIN = 'MX_SHIXIN'
        # 投资信息
        MX_investor = 'MX_investor'
        # 企业法人库数据
        CORP_INFO = 'ods_zmxpq.corp_info'
        # 企业参保情况
        ZWY_DWCBQK_XXB = 'ods_zmxpq.ZWY_DWCBQK_XXB'
        # 企业水费记录
        WATER_RATE_USAGE = 'ods_zmxpq.WATER_RATE_USAGE'
        # 企业电费记录
        ELECTRIC_CHARGE_USAGE = 'ods_zmxpq.ELECTRIC_CHARGE_USAGE'
        # 企业煤气费记录
        NATURAL_GAS_USAGE = 'ods_zmxpq.NATURAL_GAS_USAGE'

        # 上飞期初库存表 - 表头
        OPENING_INVENTORY = 'OPENING_INVENTORY'
        # 上飞期初库存表 - 表体
        OPENING_INVENTORY_DETAIL = 'OPENING_INVENTORY_DETAIL'
        # 上飞库存出入库表
        EMS_STOCK_BILL = 'EMS_STOCK_BILL'
        # 上飞交货明细表
        EMS_DELIV_DETAIL = 'EMS_DELIV_DETAIL'
        # 上飞订单明细表
        EMS_ORDER_DETAIL = 'EMS_ORDER_DETAIL'
        # 上飞订单头表
        EMS_ORDER_HEAD = 'EMS_ORDER_HEAD'
        # 上飞财务表
        EMS_FINANCE_INFO = 'EMS_FINANCE_INFO'
        # 上飞生产总表
        EMS_MANUFACTURE_TOTAL = 'EMS_MANUFACTURE_TOTAL'
        # 上飞工单头表
        EMS_WORK_HEAD = 'EMS_WORK_HEAD'
        # 上飞工单耗用表
        EMS_WORK_INPUT = 'EMS_WORK_INPUT'
        # 上飞工单产出表
        EMS_WORK_OUTPUT = 'EMS_WORK_OUTPUT'
        # 上飞加工工时耗用表
        EMS_TIMECOST_INFO = 'EMS_TIMECOST_INFO'
        #综保客户订单头表（造的）
        BD_RISK_WAREHOUSE_ORDER_HEAD = 'BD_RISK_WAREHOUSE_ORDER_HEAD'
        #综保客户订单明细表（造的）
        BD_RISK_WAREHOUSE_ORDER = 'BD_RISK_WAREHOUSE_ORDER'
        #综保出入库单
        WAREHOUSE_STOCK_BILL = 'stock_bill'
        # 海关信用公示
        CUSTOMS_CREDIT = 'ODS_ZMXPQ.CUSTOMS_CREDIT'
        # 企业信息表
        BD_RISK_CORP_INFO_BASIC = 'BD_RISK_CORP_INFO_BASIC'

        # 首页统计用到的表
        BILL_DIR_BSC = 'ODS_ZMXPQ.BILL_DIR_BSC'
        BILL_DIR_DT = 'ODS_ZMXPQ.BILL_DIR_DT'
        BILL_DIR_EXP_BSC = 'ODS_ZMXPQ.BILL_DIR_EXP_BSC'
        BILL_DIR_EXP_DT = 'ODS_ZMXPQ.BILL_DIR_EXP_DT'
        MX_DISPLAY_JINGYU = 'BD_RISK_DISPLAY_JINGYU'
        MX_DISPLAY_TAX = 'BD_RISK_DISPLAY_TAX'

        # # 模型数据
        # ST1明细表&结果表
        BD_RISK_DETAIL_STOCK_ST1 = 'BD_RISK_DETAIL_STOCK_ST1'
        BD_RISK_RESULT_STOCK_ST1 = 'BD_RISK_RESULT_STOCK_ST1'
        # ST2明细表&结果表
        BD_RISK_DETAIL_STOCK_ST2 = 'BD_RISK_DETAIL_STOCK_ST2'
        BD_RISK_RESULT_STOCK_ST2 = 'BD_RISK_RESULT_STOCK_ST2'
        # ST3明细表&结果表
        BD_RISK_DETAIL_STOCK_ST3 = 'BD_RISK_DETAIL_STOCK_ST3'
        BD_RISK_RESULT_STOCK_ST3 = 'BD_RISK_RESULT_STOCK_ST3'
        # ST4 明细表&结果表
        BD_RISK_DETAIL_STOCK_ST4 = 'BD_RISK_DETAIL_STOCK_ST4'
        BD_RISK_RESULT_STOCK_ST4 = 'BD_RISK_RESULT_STOCK_ST4'
        # ST5 明细表&结果表
        BD_RISK_DETAIL_STOCK_ST5 = 'BD_RISK_DETAIL_STOCK_ST5'
        BD_RISK_RESULT_STOCK_ST5 = 'BD_RISK_RESULT_STOCK_ST5'

        # FN1 明细表&结果表
        BD_RISK_DETAIL_FINANCE_FN1 = 'BD_RISK_DETAIL_FINANCE_FN1'
        BD_RISK_RESULT_FINANCE_FN1 = 'BD_RISK_RESULT_FINANCE_FN1'
        # FN2 明细表&结果表
        BD_RISK_DETAIL_FINANCE_FN2 = 'BD_RISK_DETAIL_FINANCE_FN2'
        BD_RISK_RESULT_FINANCE_FN2 = 'BD_RISK_RESULT_FINANCE_FN2'

        # PD1 明细表&结果表
        BD_RISK_DETAIL_PRODUCTION_PD1 = 'BD_RISK_DETAIL_PRODUCTION_PD1'
        BD_RISK_RESULT_PRODUCTION_PD1 = 'BD_RISK_RESULT_PRODUCTION_PD1'
        # PD2 明细表&结果表
        BD_RISK_DETAIL_PRODUCTION_PD2 = 'BD_RISK_DETAIL_PRODUCTION_PD2'
        BD_RISK_RESULT_PRODUCTION_PD2 = 'BD_RISK_RESULT_PRODUCTION_PD2'
        # PD3 明细表&结果表
        BD_RISK_DETAIL_PRODUCTION_PD3 = 'BD_RISK_DETAIL_PRODUCTION_PD3'
        BD_RISK_RESULT_PRODUCTION_PD3 = 'BD_RISK_RESULT_PRODUCTION_PD3'
        # PD4 明细表&结果表
        BD_RISK_DETAIL_PRODUCTION_PD4 = 'BD_RISK_DETAIL_PRODUCTION_PD4'
        BD_RISK_RESULT_PRODUCTION_PD4 = 'BD_RISK_RESULT_PRODUCTION_PD4'

        # SC1 明细表&结果表
        BD_RISK_DETAIL_SUPPLYCHAIN_SC1 = 'BD_RISK_DETAIL_SUPPLYCHAIN_SC1'
        BD_RISK_RESULT_SUPPLYCHAIN_SC1 = 'BD_RISK_RESULT_SUPPLYCHAIN_SC1'

        # WH1 明细表&结果表
        BD_RISK_DETAIL_WAREHOUSE_WH1_C = 'BD_RISK_DETAIL_WAREHOUSE_WH1_C'
        BD_RISK_DETAIL_WAREHOUSE_WH1_S = 'BD_RISK_DETAIL_WAREHOUSE_WH1_S'
        BD_RISK_RESULT_WAREHOUSE_WH1 = 'BD_RISK_RESULT_WAREHOUSE_WH1'
        # WH2 明细表&结果表
        BD_RISK_DETAIL_WAREHOUSE_WH2 = 'BD_RISK_DETAIL_WAREHOUSE_WH2'
        BD_RISK_RESULT_WAREHOUSE_WH2 = 'BD_RISK_RESULT_WAREHOUSE_WH2'

        # CR1 明细表&结果表
        BD_RISK_DETAIL_CREDIT_CR1 = 'BD_RISK_DETAIL_CREDIT_CR1'
        BD_RISK_RESULT_CREDIT_CR1 = 'BD_RISK_RESULT_CREDIT_CR1'
        # CR2 明细表&结果表
        BD_RISK_DETAIL_CREDIT_CR2 = 'BD_RISK_DETAIL_CREDIT_CR2'
        BD_RISK_RESULT_CREDIT_CR2 = 'BD_RISK_RESULT_CREDIT_CR2'
        # CR3 明细表&结果表
        BD_RISK_DETAIL_CREDIT_CR3 = 'BD_RISK_DETAIL_CREDIT_CR3'
        BD_RISK_RESULT_CREDIT_CR3 = 'BD_RISK_RESULT_CREDIT_CR3'

        # # TR1 信息表, 结果表
        BD_RISK_TRACK_INFO = 'BD_RISK_TRACK_INFO'
        BD_RISK_RESULT_TRACK_TR1 = 'BD_RISK_RESULT_TRACK_TR1'
        ## TD2 结果表
        BD_RISK_RESULT_TRADE_TD2 = 'BD_RISK_RESULT_TRADE_TD2'
        ## TD1 结果表
        BD_RISK_RESULT_TRADE_TD1 = 'BD_RISK_RESULT_TRADE_TD1'

    # # 亿通数据（测试环境暂无对应表）
        # 进口海运报关单表头
        FT_I_DTL_SEA_PRE_RECORDED = 'DW_I_BASIC.FT_I_DTL_SEA_PRE_RECORDED'
        # 进口其他报关单表头
        FT_I_DTL_OTR_PRE_RECORDED = 'DW_I_BASIC.FT_I_DTL_OTR_PRE_RECORDED'
        # 进口海运报关单商品信息表
        FT_I_DTL_SEA_LIST = 'DW_I_BASIC.FT_I_DTL_SEA_LIST'
        # 进口其他报关单商品信息表
        FT_I_DTL_OTR_LIST = 'DW_I_BASIC.FT_I_DTL_OTR_LIST'
        # 进口海运箱单
        FT_I_DTL_SEA_CONTAINER = 'DW_I_BASIC.FT_I_DTL_SEA_CONTAINER'
        # 进口海运COARRI
        FT_I_DTL_COARRI_CTNR = 'DW_I_BASIC.FT_I_DTL_COARRI_CTNR'

        # 出口海运报关单表头
        FT_E_DTL_SEA_PRE_RECORDED = 'DW_E_BASIC.FT_E_DTL_SEA_PRE_RECORDED'
        # 出口其他报关单表头
        FT_E_DTL_OTR_PRE_RECORDED = 'DW_E_BASIC.FT_E_DTL_OTR_PRE_RECORDED'
        # 出口海运报关单商品信息表
        FT_E_DTL_SEA_LIST = 'DW_E_BASIC.FT_E_DTL_SEA_LIST'
        # 出口其他报关单商品信息表
        FT_E_DTL_OTR_LIST = 'DW_E_BASIC.FT_E_DTL_OTR_LIST'
        # 出口海运箱子
        FT_E_DTL_SEA_CONTAINER = 'DW_E_BASIC.FT_E_DTL_SEA_CONTAINER'
        # 出口海运COARRI
        FT_E_DTL_COARRI_CTNR = 'DW_E_BASIC.FT_E_DTL_COARRI_CTNR'

        FT_I_DTL_TAX_INFO = 'DW_I_BASIC.FT_I_DTL_TAX_INFO'
        FT_E_DTL_TAX_INFO = 'DW_E_BASIC.FT_E_DTL_TAX_INFO'

        # 承运人码表
        DIM_OPERATOR = 'DIM.DIM_OPERATOR'
        # 企业列表（洋山围网）
        DIM_FTZ_CORP = 'DIM.DIM_FTZ_CORP'
        # 报关单经营单位信息表
        DIM_TRADER = 'DIM.DIM_TRADER'
        # 国家代码码表
        DIM_COUNTRY = 'DIM.DIM_COUNTRY'
        # 币制码表
        MAP_GJ_CURRENCY = 'DIM.MAP_GJ_CURRENCY'

        # # # BVD数据
        MX_BVD = 'MX_BVD'
        # 进出口报关单统计表
        FT_CUS_DWS_TRADE = 'DW_STA.FT_CUS_DWS_TRADE'
        # 指标货类表
        FT_STA_GOODSOWNER_MAIN_CLASS = 'DW_STA.FT_STA_GOODSOWNER_MAIN_CLASS'
        # 企业oneid表
        DW_CORP_CUSDEC = 'dw_corp_basic.dw_corp_cusdec'
        # 报关单表
        FT_CUS_DWS_ENTRY = 'DW_STA.FT_CUS_DWS_ENTRY'

    else:
        # 生产环境
        # oracle-ods
        # 日志表
        BD_RISK_MODEL_LOG = 'BD_RISK_MODEL_LOG'
        # 大模块分数表
        BD_RISK_CORP_SCORE_DISPLAY = 'BD_RISK_CORP_SCORE_DISPLAY'
        # 预警表
        BD_RISK_ALARM_ITEM ='BD_RISK_ALARM_ITEM'

        # # 亿通数据（测试环境暂无对应表）
        # 进口海运报关单表头
        FT_I_DTL_SEA_PRE_RECORDED = 'DW_I_BASIC.FT_I_DTL_SEA_PRE_RECORDED'
        # 进口其他报关单表头
        FT_I_DTL_OTR_PRE_RECORDED = 'DW_I_BASIC.FT_I_DTL_OTR_PRE_RECORDED'
        # 进口海运报关单商品信息表
        FT_I_DTL_SEA_LIST = 'DW_I_BASIC.FT_I_DTL_SEA_LIST'
        # 进口其他报关单商品信息表
        FT_I_DTL_OTR_LIST = 'DW_I_BASIC.FT_I_DTL_OTR_LIST'
        # 进口海运箱单
        FT_I_DTL_SEA_CONTAINER = 'DW_I_BASIC.FT_I_DTL_SEA_CONTAINER'
        # 进口海运COARRI
        FT_I_DTL_COARRI_CTNR = 'DW_I_BASIC.FT_I_DTL_COARRI_CTNR'

        # 出口海运报关单表头
        FT_E_DTL_SEA_PRE_RECORDED = 'DW_E_BASIC.FT_E_DTL_SEA_PRE_RECORDED'
        # 出口其他报关单表头
        FT_E_DTL_OTR_PRE_RECORDED = 'DW_E_BASIC.FT_E_DTL_OTR_PRE_RECORDED'
        # 出口海运报关单商品信息表
        FT_E_DTL_SEA_LIST = 'DW_E_BASIC.FT_E_DTL_SEA_LIST'
        # 出口其他报关单商品信息表
        FT_E_DTL_OTR_LIST = 'DW_E_BASIC.FT_E_DTL_OTR_LIST'
        # 出口海运箱子
        FT_E_DTL_SEA_CONTAINER = 'DW_E_BASIC.FT_E_DTL_SEA_CONTAINER'
        # 出口海运COARRI
        FT_E_DTL_COARRI_CTNR = 'DW_E_BASIC.FT_E_DTL_COARRI_CTNR'

        # 进出口报关单统计表
        FT_CUS_DWS_TRADE = 'DW_STA.FT_CUS_DWS_TRADE'
        # 进出口海关税单
        FT_I_DTL_TAX_INFO = 'DW_I_BASIC.FT_I_DTL_TAX_INFO'
        FT_E_DTL_TAX_INFO = 'DW_E_BASIC.FT_E_DTL_TAX_INFO'
        # 指标货类表
        FT_STA_GOODSOWNER_MAIN_CLASS = 'DW_STA.FT_STA_GOODSOWNER_MAIN_CLASS'
        # 企业oneid表
        DW_CORP_CUSDEC = 'dw_corp_basic.dw_corp_cusdec'
        # 报关单表
        FT_CUS_DWS_ENTRY = 'DW_STA.FT_CUS_DWS_ENTRY'
        # 承运人码表
        DIM_OPERATOR = 'DIM.DIM_OPERATOR'
        # 企业列表（洋山围网）
        DIM_FTZ_CORP = 'DIM.DIM_FTZ_CORP'
        # 报关单经营单位信息表
        DIM_TRADER = 'DIM.DIM_TRADER'
        # 国家代码码表
        DIM_COUNTRY = 'DIM.DIM_COUNTRY'
        # 币制码表
        MAP_GJ_CURRENCY = 'DIM.MAP_GJ_CURRENCY'

        # 企业信息列表
        ft_gov_dtl_corp_info = 'dw_lgxc_basic.ft_gov_dtl_corp_info'
        # 涉诉信息
        MX_SHESU = 'MX_SHESU'
        # 失信信息
        MX_SHIXIN = 'MX_SHIXIN'
        # 投资信息
        MX_investor = 'MX_investor'
        # 企业法人库数据
        CORP_INFO = 'ods_zmxpq.corp_info'
        # 企业参保情况
        ZWY_DWCBQK_XXB = 'ods_zmxpq.ZWY_DWCBQK_XXB'
        # 企业水费记录
        WATER_RATE_USAGE = 'ods_zmxpq.WATER_RATE_USAGE'
        # 企业电费记录
        ELECTRIC_CHARGE_USAGE = 'ods_zmxpq.ELECTRIC_CHARGE_USAGE'
        # 企业煤气费记录
        NATURAL_GAS_USAGE = 'ods_zmxpq.NATURAL_GAS_USAGE'
        # 海关信用公示
        CUSTOMS_CREDIT = 'ODS_ZMXPQ.CUSTOMS_CREDIT'
        # 企业信息表
        BD_RISK_CORP_INFO_BASIC = 'BD_RISK.BD_RISK_CORP_INFO_BASIC'
        # # BVD数据
        MX_BVD = 'MX_BVD'

        # # 企业ERP数据
        # 上飞期初库存表 - 表头
        OPENING_INVENTORY = 'OPENING_INVENTORY'
        # 上飞期初库存表 - 表体
        OPENING_INVENTORY_DETAIL = 'OPENING_INVENTORY_DETAIL'
        # 上飞库存出入库表
        EMS_STOCK_BILL = 'ODS_ZMXPQ.EMS_STOCK_BILL'
        # 上飞交货明细表
        EMS_DELIV_DETAIL = 'ods_zmxpq.EMS_DELIV_DETAIL'
        # 上飞订单明细表
        EMS_ORDER_DETAIL = 'ods_zmxpq.EMS_ORDER_DETAIL'
        # 上飞订单头表
        EMS_ORDER_HEAD = 'ods_zmxpq.EMS_ORDER_HEAD'
        # 上飞财务表
        EMS_FINANCE_INFO = 'ods_zmxpq.EMS_FINANCE_INFO'
        # 上飞生产总表
        EMS_MANUFACTURE_TOTAL = 'ods_zmxpq.EMS_MANUFACTURE_TOTAL'
        # 上飞工单头表
        EMS_WORK_HEAD = 'ods_zmxpq.EMS_WORK_HEAD'
        # 上飞工单耗用表
        EMS_WORK_INPUT = 'ods_zmxpq.EMS_WORK_INPUT'
        # 上飞工单产出表
        EMS_WORK_OUTPUT = 'ods_zmxpq.EMS_WORK_OUTPUT'
        # 上飞加工工时耗用表
        EMS_TIMECOST_INFO = 'ods_zmxpq.EMS_TIMECOST_INFO'
        # 综保客户订单头表（造的）
        BD_RISK_WAREHOUSE_ORDER_HEAD = 'BD_RISK_WAREHOUSE_ORDER_HEAD'
        # 综保客户订单明细表（造的）
        BD_RISK_WAREHOUSE_ORDER = 'BD_RISK_WAREHOUSE_ORDER'
        # 综保出入库单
        WAREHOUSE_STOCK_BILL = 'ods_zmxpq.stock_bill'

        # 首页统计用到的表
        BILL_DIR_BSC = 'ODS_ZMXPQ.BILL_DIR_BSC'
        BILL_DIR_DT = 'ODS_ZMXPQ.BILL_DIR_DT'
        BILL_DIR_EXP_BSC = 'ODS_ZMXPQ.BILL_DIR_EXP_BSC'
        BILL_DIR_EXP_DT = 'ODS_ZMXPQ.BILL_DIR_EXP_DT'
        MX_DISPLAY_JINGYU = 'BD_RISK_DISPLAY_JINGYU'
        MX_DISPLAY_TAX = 'BD_RISK_DISPLAY_TAX'


        # # 模型数据
        # ST1明细表&结果表
        BD_RISK_DETAIL_STOCK_ST1 = 'BD_RISK_DETAIL_STOCK_ST1'
        BD_RISK_RESULT_STOCK_ST1 = 'BD_RISK_RESULT_STOCK_ST1'
        # ST2明细表&结果表
        BD_RISK_DETAIL_STOCK_ST2 = 'BD_RISK_DETAIL_STOCK_ST2'
        BD_RISK_RESULT_STOCK_ST2 = 'BD_RISK_RESULT_STOCK_ST2'
        # ST3明细表&结果表
        BD_RISK_DETAIL_STOCK_ST3 = 'BD_RISK_DETAIL_STOCK_ST3'
        BD_RISK_RESULT_STOCK_ST3 = 'BD_RISK_RESULT_STOCK_ST3'
        # ST4 明细表&结果表
        BD_RISK_DETAIL_STOCK_ST4 = 'BD_RISK_DETAIL_STOCK_ST4'
        BD_RISK_RESULT_STOCK_ST4 = 'BD_RISK_RESULT_STOCK_ST4'
        # ST5 明细表&结果表
        BD_RISK_DETAIL_STOCK_ST5 = 'BD_RISK_DETAIL_STOCK_ST5'
        BD_RISK_RESULT_STOCK_ST5 = 'BD_RISK_RESULT_STOCK_ST5'

        # FN1 明细表&结果表
        BD_RISK_DETAIL_FINANCE_FN1 = 'BD_RISK_DETAIL_FINANCE_FN1'
        BD_RISK_RESULT_FINANCE_FN1 = 'BD_RISK_RESULT_FINANCE_FN1'
        BD_RISK_GRAPH_FINANCE_FN1 = 'BD_RISK_GRAPH_FINANCE_FN1'
        # FN2 明细表&结果表
        BD_RISK_DETAIL_FINANCE_FN2 = 'BD_RISK_DETAIL_FINANCE_FN2'
        BD_RISK_RESULT_FINANCE_FN2 = 'BD_RISK_RESULT_FINANCE_FN2'

        # PD1 明细表&结果表
        BD_RISK_DETAIL_PRODUCTION_PD1 = 'BD_RISK_DETAIL_PRODUCTION_PD1'
        BD_RISK_RESULT_PRODUCTION_PD1 = 'BD_RISK_RESULT_PRODUCTION_PD1'
        # PD2 明细表&结果表
        BD_RISK_DETAIL_PRODUCTION_PD2 = 'BD_RISK_DETAIL_PRODUCTION_PD2'
        BD_RISK_RESULT_PRODUCTION_PD2 = 'BD_RISK_RESULT_PRODUCTION_PD2'
        # PD3 明细表&结果表
        BD_RISK_DETAIL_PRODUCTION_PD3 = 'BD_RISK_DETAIL_PRODUCTION_PD3'
        BD_RISK_RESULT_PRODUCTION_PD3 = 'BD_RISK_RESULT_PRODUCTION_PD3'
        # PD4 明细表&结果表
        BD_RISK_DETAIL_PRODUCTION_PD4 = 'BD_RISK_DETAIL_PRODUCTION_PD4'
        BD_RISK_RESULT_PRODUCTION_PD4 = 'BD_RISK_RESULT_PRODUCTION_PD4'

        # SC1 明细表&结果表
        BD_RISK_DETAIL_SUPPLYCHAIN_SC1 = 'BD_RISK_DETAIL_SUPPLYCHAIN_SC1'
        BD_RISK_RESULT_SUPPLYCHAIN_SC1 = 'BD_RISK_RESULT_SUPPLYCHAIN_SC1'

        # WH1 明细表&结果表
        BD_RISK_DETAIL_WAREHOUSE_WH1_C = 'BD_RISK_DETAIL_WAREHOUSE_WH1_C'
        BD_RISK_DETAIL_WAREHOUSE_WH1_S = 'BD_RISK_DETAIL_WAREHOUSE_WH1_S'
        BD_RISK_RESULT_WAREHOUSE_WH1 = 'BD_RISK_RESULT_WAREHOUSE_WH1'
        # WH2 明细表&结果表
        BD_RISK_DETAIL_WAREHOUSE_WH2 = 'BD_RISK_DETAIL_WAREHOUSE_WH2'
        BD_RISK_RESULT_WAREHOUSE_WH2 = 'BD_RISK_RESULT_WAREHOUSE_WH2'

        # CR1
        BD_RISK_DETAIL_CREDIT_CR1 = 'BD_RISK_DETAIL_CREDIT_CR1'
        BD_RISK_RESULT_CREDIT_CR1 = 'BD_RISK_RESULT_CREDIT_CR1'
        # CR2
        BD_RISK_DETAIL_CREDIT_CR2 = 'BD_RISK_DETAIL_CREDIT_CR2'
        BD_RISK_RESULT_CREDIT_CR2 = 'BD_RISK_RESULT_CREDIT_CR2'
        # CR3 明细表&结果表
        BD_RISK_DETAIL_CREDIT_CR3 = 'BD_RISK_DETAIL_CREDIT_CR3'
        BD_RISK_RESULT_CREDIT_CR3 = 'BD_RISK_RESULT_CREDIT_CR3'

        # # TR1 信息表, 结果表
        BD_RISK_TRACK_INFO = 'BD_RISK_TRACK_INFO'
        BD_RISK_RESULT_TRACK_TR1 = 'BD_RISK_RESULT_TRACK_TR1'

        ## TD2 结果表
        BD_RISK_RESULT_TRADE_TD2 = 'BD_RISK_RESULT_TRADE_TD2'
        ## TD1 结果表
        BD_RISK_RESULT_TRADE_TD1 = 'BD_RISK_RESULT_TRADE_TD1'

        #公共的电费与社保结果表 旧风控用
        MX_PUBLIC_ELECTRIC = 'BD_RISK.MX_PUBLIC_ELECTRIC'
        MX_PUBLIC_INSURANCE = 'BD_RISK.MX_PUBLIC_INSURANCE'

