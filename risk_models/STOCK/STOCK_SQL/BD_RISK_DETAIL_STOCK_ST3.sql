CREATE TABLE BD_RISK_DETAIL_STOCK_ST3 (
ID NUMBER(20) NOT NULL ,
ORG_CODE VARCHAR2(64 CHAR) NOT NULL ,
QTY NUMBER(20,5) NULL,
QTY_CO NUMBER(20,5) NULL,
UNIT_CO VARCHAR2(64 CHAR) NULL,
ACTRUAL_STOCK_DATE DATE NULL,
TRADE_TOTAL NUMBER(20,5) NULL,
BILL_TYPE VARCHAR2(64 CHAR) NULL,
NET_WT NUMBER(20,5) NULL,
CHECK_TIME DATE NOT NULL,
ISCURRENT NUMBER(10) NOT NULL,
LASTUPDATE DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.ORG_CODE IS '经营企业单位编码';
COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.QTY IS '申报货物数量';
COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.QTY_CO IS '企业货物数量';
COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.UNIT_CO IS '企业计量单位';
COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.ACTRUAL_STOCK_DATE IS '实际出入库时间';
COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.TRADE_TOTAL IS '总价';
COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.BILL_TYPE IS '出入库类型';
COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.NET_WT IS '重量';
COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.CHECK_TIME IS '模型运行时间';
COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.ISCURRENT IS '是否使用';
COMMENT ON COLUMN BD_RISK_DETAIL_STOCK_ST3.LASTUPDATE IS '数据最后更新时间';


-- ----------------------------
-- Indexes structure for table BD_RISK_DETAIL_STOCK_ST3
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_DETAIL_STOCK_ST3
-- ----------------------------
ALTER TABLE BD_RISK_DETAIL_STOCK_ST3 ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_DETAIL_STOCK_ST3 ADD PRIMARY KEY (ID);