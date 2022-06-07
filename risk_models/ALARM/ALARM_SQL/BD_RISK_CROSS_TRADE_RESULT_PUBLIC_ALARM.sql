CREATE TABLE BD_RISK_CROSS_TRADE_RESULT_PUBLIC_ALARM (
ID NUMBER(20) NOT NULL,
CREDIT_CODE VARCHAR2(64 CHAR) NULL,
I_E_FLAG VARCHAR2(1 CHAR) NULL,
MODEL_TIME DATE NULL,
PUBLIC_A NUMBER(20,3) NULL,
SCORE NUMBER(10) NULL,
ISCURRENT NUMBER(1) NULL,
LASTUPDATE DATE NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;


-- ----------------------------
-- Indexes structure for table BD_RISK_DETAIL_STOCK_ST1
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_DETAIL_STOCK_ST1
-- ----------------------------
ALTER TABLE BD_RISK_CROSS_TRADE_RESULT_PUBLIC_ALARM ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_CROSS_TRADE_RESULT_PUBLIC_ALARM ADD PRIMARY KEY (ID);