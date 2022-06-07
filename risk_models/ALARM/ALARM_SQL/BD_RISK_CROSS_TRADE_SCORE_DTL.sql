CREATE TABLE BD_RISK_CROSS_TRADE_SCORE_DTL (
ID NUMBER(20) NOT NULL ,
ORG_CODE VARCHAR2(64 CHAR) NOT NULL ,
HG_ORG_ID VARCHAR2(64 CHAR) NULL ,
BUSINESS_TIME DATE NOT NULL ,
MODEL_CODE VARCHAR2(64 CHAR) NOT NULL ,
CHILD_MODEL_CODE VARCHAR2(64 CHAR) NOT NULL ,
LABEL VARCHAR2(64 CHAR) NOT NULL ,
EVENT_NUMBER NUMBER(20) NOT NULL ,
SCORE NUMBER(20,5) NOT NULL ,
ISCURRENT NUMBER(10) NOT NULL ,
LASTUPDATE DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_CROSS_TRADE_SCORE_DTL.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_SCORE_DTL.ORG_CODE IS '企业18位社会信用代码';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_SCORE_DTL.HG_ORG_ID IS '海关企业代码';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_SCORE_DTL.BUSINESS_TIME IS '统计时间';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_SCORE_DTL.MODEL_CODE IS '维度代码';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_SCORE_DTL.CHILD_MODEL_CODE IS '子模型代码';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_SCORE_DTL.LABEL IS '风控标签(异常类型/扣分原因)';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_SCORE_DTL.SCORE IS '维度分数';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_SCORE_DTL.EVENT_NUMBER IS '事件数量';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_SCORE_DTL.ISCURRENT IS '是否使用：1(使用)；0(不使用)';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_SCORE_DTL.LASTUPDATE IS '数据最后更新时间';


-- ----------------------------
-- Indexes structure for table BD_RISK_CROSS_TRADE_SCORE_DTL
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_CROSS_TRADE_SCORE_DTL
-- ----------------------------
ALTER TABLE BD_RISK_CROSS_TRADE_SCORE_DTL ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_CROSS_TRADE_SCORE_DTL ADD PRIMARY KEY (ID);