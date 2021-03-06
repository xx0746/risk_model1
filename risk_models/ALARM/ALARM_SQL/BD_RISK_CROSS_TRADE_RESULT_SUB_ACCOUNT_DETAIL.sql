CREATE TABLE BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL (
ID NUMBER(20) NOT NULL,
I_IE_TYPECD VARCHAR2(1 CHAR) NULL,
I_SUB_ALARM_FK NUMBER(10) NULL,
I_BIZOP_ETPS_SCCD VARCHAR2(64 CHAR) NULL,
I_BIZOP_ETPS_NM VARCHAR2(64 CHAR),
I_ORG_ID VARCHAR2(64 CHAR) NULL,
I_TYPE VARCHAR2(64 CHAR) NULL,
I_MASTER_CUSCD VARCHAR2(64 CHAR) NULL,
I_RLT_WH_REC_NO VARCHAR2(64 CHAR),
I_WH_REC_NO VARCHAR2(64 CHAR),
I_DCL_TIME VARCHAR2(64 CHAR),
E_IE_TYPECD VARCHAR2(1 CHAR) NULL,
E_SUB_ALARM_FK NUMBER(10) NULL,
E_BIZOP_ETPS_SCCD VARCHAR2(64 CHAR) NULL,
E_BIZOP_ETPS_NM VARCHAR2(64 CHAR),
E_ORG_ID VARCHAR2(64 CHAR) NULL,
E_TYPE VARCHAR2(64 CHAR) NULL,
E_MASTER_CUSCD VARCHAR2(64 CHAR) NULL,
E_RLT_WH_REC_NO VARCHAR2(64 CHAR),
E_WH_REC_NO VARCHAR2(64 CHAR),
E_DCL_TIME VARCHAR2(64 CHAR),
MODEL_TIME DATE NULL,
ISCURRENT NUMBER(1) NULL,
LASTUPDATE DATE NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.I_BIZOP_ETPS_SCCD IS '转入方公司18位信用代码';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.I_ORG_ID IS '转入方子账户账号';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.I_TYPE IS '转入方子账户类型';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.I_MASTER_CUSCD IS '转入方主管海关';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.I_BIZOP_ETPS_SCCD IS '转出方公司18位信用代码';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.I_ORG_ID IS '转出方子账户账号';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.I_TYPE IS '转出方子账户类型';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.I_MASTER_CUSCD IS '转出方主管海关';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.DCL_TIME IS '业务时间（申报日期';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.MODEL_TIME IS '该数据的处理时间';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.ISCURRENT IS '是否使用：1(使用)；0(不使用)';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL.LASTUPDATE IS '数据最后更新时间';


-- ----------------------------
-- Indexes structure for table BD_RISK_DETAIL_STOCK_ST1
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_DETAIL_STOCK_ST1
-- ----------------------------
ALTER TABLE BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_CROSS_TRADE_RESULT_SUB_ACCOUNT_DETAIL ADD PRIMARY KEY (ID);