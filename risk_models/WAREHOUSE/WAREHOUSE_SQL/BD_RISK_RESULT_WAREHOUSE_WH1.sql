CREATE TABLE BD_RISK_RESULT_WAREHOUSE_WH1 (
ID NUMBER(20) NOT NULL ,
ORG_CODE VARCHAR2(64 CHAR) NOT NULL ,
CUSTOMS_CODE VARCHAR2(32 CHAR) NULL ,
STOCK_BILL_TYPE VARCHAR2(32 CHAR) NULL ,
STOCK_TYPE VARCHAR2(32 CHAR) NULL ,
BILL_TYPE VARCHAR2(32 CHAR) NULL ,
BUSINESS_TYPE VARCHAR2(32 CHAR) NULL ,
BILL_STATUS VARCHAR2(32 CHAR) NULL ,
CLASSIFY_TYPE VARCHAR2(32 CHAR) NULL ,
SUPV_MODE VARCHAR2(32 CHAR) NULL ,
I_E_PORT VARCHAR2(32 CHAR) NULL,
TRAF_MODE VARCHAR2(16 CHAR) NULL,
ENTRY_NO VARCHAR2(64 CHAR) NOT NULL,
ENTRY_GDS_SEQNO NUMBER(20) NOT NULL,
HS_CODE VARCHAR2(64 CHAR) NULL,
G_NAME VARCHAR2(64 CHAR) NULL,
G_MODEL VARCHAR2(512 CHAR) NULL,
ORIGIN_COUNTRY_CODE VARCHAR2(64 CHAR) NULL,
QTY NUMBER(20,5) NULL,
G_UNIT VARCHAR2(32 CHAR) NULL,
QTY_1 NUMBER(20,5) NULL,
UNIT_1 VARCHAR2(32 CHAR) NULL,
QTY_2 NUMBER(20,5) NULL,
UNIT_2 VARCHAR2(32 CHAR) NULL,
TRADE_CURR VARCHAR2(32 CHAR) NULL,
TRADE_TOTAL NUMBER(20,5) NULL,
STARTDT DATE NULL,
RISK_LABEL VARCHAR2(64 CHAR) NOT NULL,
SCORE NUMBER(20,5) NOT NULL,
CHECK_TIME DATE NOT NULL,
ISCURRENT NUMBER(10) NOT NULL,
LASTUPDATE DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.ORG_CODE IS '公司社会信用代码';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.CUSTOMS_CODE IS '海关关区代码';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.STOCK_BILL_TYPE IS '出入库单进出类型';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.STOCK_TYPE IS '库存类型';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.BILL_TYPE IS '出入库类型';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.BUSINESS_TYPE IS '业务类别';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.BILL_STATUS IS '当前记录出入库状态';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.CLASSIFY_TYPE IS '料件成品标志';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.SUPV_MODE IS '监管方式';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.I_E_PORT IS '进出口岸';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.TRAF_MODE IS '运输方式';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.ENTRY_NO IS '报关单号';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.ENTRY_GDS_SEQNO IS '报关单中货物序号';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.HS_CODE IS 'HSCODE';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.G_NAME IS '货物品名';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.G_MODEL IS '货物型号';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.ORIGIN_COUNTRY_CODE IS '原产国代码';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.QTY IS '货物数量';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.G_UNIT IS '货物单位代码';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.QTY_1 IS '第一货物数量';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.UNIT_1 IS '第一货物单位代码';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.QTY_2 IS '第二货物数量';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.UNIT_2 IS '第二货物单位代码';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.TRADE_CURR IS '币制代码';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.TRADE_TOTAL IS '总价';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.STARTDT IS '开始时间';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.RISK_LABEL IS '风控标签';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.SCORE IS '分数';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.CHECK_TIME IS '运行时间';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.ISCURRENT IS '是否使用';
COMMENT ON COLUMN BD_RISK_RESULT_WAREHOUSE_WH1.LASTUPDATE IS '数据最后更新时间';


-- ----------------------------
-- Indexes structure for table BD_RISK_RESULT_WAREHOUSE_WH1
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_RESULT_WAREHOUSE_WH1
-- ----------------------------
ALTER TABLE BD_RISK_RESULT_WAREHOUSE_WH1 ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_RESULT_WAREHOUSE_WH1 ADD PRIMARY KEY (ID);