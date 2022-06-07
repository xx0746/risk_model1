CREATE TABLE BD_RISK_CROSS_TRADE_DEAL_WITH_ASSESSMENT (
ID NUMBER(20) NOT NULL ,
ORG_ID VARCHAR2(64 CHAR) NULL,
BUSINESS_TIME DATE NULL,
NOT_RESOLVE VARCHAR2(64 CHAR) NULL,
RESOLVE VARCHAR2(64 CHAR) NULL,
TYPE VARCHAR2(64 CHAR) NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_CROSS_TRADE_DEAL_WITH_ASSESSMENT.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_DEAL_WITH_ASSESSMENT.ORG_ID IS '企业18位社会信用代码';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_DEAL_WITH_ASSESSMENT.BUSINESS_TIME IS '业务时间';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_DEAL_WITH_ASSESSMENT.NOT_RESOLVE IS '未处置';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_DEAL_WITH_ASSESSMENT.RESOLVE IS '已处置';
COMMENT ON COLUMN BD_RISK_CROSS_TRADE_DEAL_WITH_ASSESSMENT.TYPE IS '类型';


-- ----------------------------
-- Indexes structure for table BD_RISK_CROSS_TRADE_SCORE
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_CROSS_TRADE_SCORE
-- ----------------------------
ALTER TABLE BD_RISK_CROSS_TRADE_DEAL_WITH_ASSESSMENT ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_CROSS_TRADE_DEAL_WITH_ASSESSMENT ADD PRIMARY KEY (ID);
