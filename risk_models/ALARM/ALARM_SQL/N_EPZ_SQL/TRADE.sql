CREATE TABLE TRADE (
ID NUMBER(20) NOT NULL ,
CREDIT_CODE VARCHAR2(64 CHAR) NULL,
TRADE_CODE VARCHAR2(64 CHAR) NULL,
TRADE_NAME VARCHAR2(64 CHAR) NULL,
LABEL_CODE VARCHAR2(64 CHAR) NULL,
ISCURRENT NUMBER(1) NULL,
LASTUPDATE timestamp NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;


-- ----------------------------
-- Indexes structure for table BD_RISK_CROSS_TRADE_SCORE
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_CROSS_TRADE_SCORE
-- ----------------------------
ALTER TABLE TRADE ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE TRADE ADD PRIMARY KEY (ID);