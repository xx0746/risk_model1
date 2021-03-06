CREATE TABLE BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN (
ID NUMBER(20) NOT NULL,
ENTRY_ID VARCHAR2(64 CHAR) NULL,
D_DATE VARCHAR2(64 CHAR) NULL,
MODEL_TIME DATE NULL,
OWNER_CODE_SCC VARCHAR2(64 CHAR) NULL,
OWNER_CODE VARCHAR2(64 CHAR) NULL,
OWNER_NAME VARCHAR2(64 CHAR) NULL,
CONSIGN_SCC VARCHAR2(64 CHAR) NULL,
CONSIGN_CODE VARCHAR2(64 CHAR) NULL,
CONSIGN_NAME VARCHAR2(64 CHAR) NULL,
WH_REC_PREENT_NO VARCHAR2(64 CHAR) NULL,
NOTE_S VARCHAR2(4000 CHAR) NULL,
ISCURRENT NUMBER(1) NULL,
LASTUPDATE timestamp NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN.D_DATE IS '业务时间';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN.MODEL_TIME IS '该数据的处理时间';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN.CREDIT_CODE IS '经营单位代码18位';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN.ENTRY_ID IS '报关单号';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN.WH_REC_PREENT_NO IS '提发货凭证号';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN.OWNER_CODE IS '收发货人代码';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN.NOTE_S IS '报关单备注栏';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN.ISCURRENT IS '是否使用：1(使用)；0(不使用)';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN.LASTUPDATE IS '数据最后更新时间';


-- ----------------------------
-- Indexes structure for table BD_RISK_DETAIL_STOCK_ST1
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_DETAIL_STOCK_ST1
-- ----------------------------
ALTER TABLE BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_CROSS_TRADE_RESULT_FUTURE_WAREHOUSE_CLEAN ADD PRIMARY KEY (ID);