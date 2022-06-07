CREATE TABLE BD_RISK_RESULT_STOCK_ST1 (
ID NUMBER(20) NOT NULL ,
ORG_CODE VARCHAR2(64 CHAR) NOT NULL ,
STARTDT DATE NOT NULL ,
ENDDT DATE NOT NULL ,
WH_NO VARCHAR2(128 CHAR) NULL ,
WH_LOC VARCHAR2(128 CHAR) NULL ,
COP_G_NO VARCHAR2(128 CHAR) NULL ,
RISK_LABEL VARCHAR2(64 CHAR) NOT NULL ,
SCORE NUMBER(20,5) NOT NULL ,
CHECK_TIME DATE NOT NULL ,
ISCURRENT NUMBER(10) NOT NULL ,
LASTUPDATE DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.ORG_CODE IS '经营企业18位信用代码';
COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.STARTDT IS '期初时间';
COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.ENDDT IS '期末时间';
COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.WH_NO IS '仓库编码';
COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.WH_LOC IS '库位编码';
COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.COP_G_NO IS '料号';
COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.RISK_LABEL IS '风险标签';
COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.SCORE IS '分数';
COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.CHECK_TIME IS '模型运行时间';
COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.ISCURRENT IS '是否使用：1(使用)；0(不使用)';
COMMENT ON COLUMN BD_RISK_RESULT_STOCK_ST1.LASTUPDATE IS '数据最后更新时间';


-- ----------------------------
-- Indexes structure for table BD_RISK_RESULT_STOCK_ST1
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_RESULT_STOCK_ST1
-- ----------------------------
ALTER TABLE BD_RISK_RESULT_STOCK_ST1 ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_RESULT_STOCK_ST1 ADD PRIMARY KEY (ID);