CREATE TABLE BD_RISK_DETAIL_PRODUCTION_PD3 (
ID NUMBER(20) NOT NULL ,
ORG_CODE VARCHAR2(64 CHAR) NOT NULL ,
WO_NO VARCHAR2(128 CHAR) NOT NULL ,
RLT_BILL_DETAIL_SEQNO VARCHAR2(128 CHAR) NOT NULL ,
COP_G_NO VARCHAR2(128 CHAR) NOT NULL ,
BATCH_TYPE VARCHAR2(128 CHAR) NOT NULL ,
OUTSTOCK_NUM NUMBER(20,5) NULL ,
CONSUME_NUM NUMBER(20,5) NULL ,
BACKSTOCK_NUM NUMBER(20,5) NULL ,
RATIO NUMBER(20,5) NULL ,
CHECK_TIME DATE NOT NULL,
ISCURRENT NUMBER(10) NOT NULL,
LASTUPDATE DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.ORG_CODE IS '公司社会信用代码';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.WO_NO IS '工单号';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.RLT_BILL_DETAIL_SEQNO IS '关联单号';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.COP_G_NO IS '料号';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.BATCH_TYPE IS '批次类型';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.OUTSTOCK_NUM IS '领用数量';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.CONSUME_NUM IS '消耗数量';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.BACKSTOCK_NUM IS '退库数量';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.RATIO IS '比例';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.CHECK_TIME IS '运行时间';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.ISCURRENT IS '是否使用';
COMMENT ON COLUMN BD_RISK_DETAIL_PRODUCTION_PD3.LASTUPDATE IS '数据最后更新时间';


-- ----------------------------
-- Indexes structure for table BD_RISK_DETAIL_PRODUCTION_PD3
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_DETAIL_PRODUCTION_PD3
-- ----------------------------
ALTER TABLE BD_RISK_DETAIL_PRODUCTION_PD3 ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_DETAIL_PRODUCTION_PD3 ADD PRIMARY KEY (ID);