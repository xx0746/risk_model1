CREATE TABLE BD_RISK_RESULT_PRODUCTION_PD1 (
ID NUMBER(20) NOT NULL ,
ORG_CODE VARCHAR2(64 CHAR) NOT NULL ,
BATCH_NO VARCHAR2(128 CHAR) NOT NULL ,
WO_NO VARCHAR2(128 CHAR) NOT NULL ,
RISK_LABEL VARCHAR2(64 CHAR) NULL ,
SCORE NUMBER(20,5) NULL ,
CHECK_TIME DATE NOT NULL ,
ISCURRENT NUMBER(10) NOT NULL ,
LASTUPDATE DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_RESULT_PRODUCTION_PD1.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_RESULT_PRODUCTION_PD1.ORG_CODE IS '经营企业18位信用代码';
COMMENT ON COLUMN BD_RISK_RESULT_PRODUCTION_PD1.BATCH_NO IS '批次号';
COMMENT ON COLUMN BD_RISK_RESULT_PRODUCTION_PD1.WO_NO IS '工单号';
COMMENT ON COLUMN BD_RISK_RESULT_PRODUCTION_PD1.RISK_LABEL IS '风险标签';
COMMENT ON COLUMN BD_RISK_RESULT_PRODUCTION_PD1.SCORE IS '分数';
COMMENT ON COLUMN BD_RISK_RESULT_PRODUCTION_PD1.CHECK_TIME IS '模型运行时间';
COMMENT ON COLUMN BD_RISK_RESULT_PRODUCTION_PD1.ISCURRENT IS '是否使用：1(使用)；0(不使用)';
COMMENT ON COLUMN BD_RISK_RESULT_PRODUCTION_PD1.LASTUPDATE IS '数据最后更新时间';


-- ----------------------------
-- Indexes structure for table BD_RISK_RESULT_PRODUCTION_PD1
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_RESULT_PRODUCTION_PD1
-- ----------------------------
ALTER TABLE BD_RISK_RESULT_PRODUCTION_PD1 ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_RESULT_PRODUCTION_PD1 ADD PRIMARY KEY (ID);