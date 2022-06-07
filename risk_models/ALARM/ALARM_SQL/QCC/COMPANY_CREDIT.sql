create table COMPANY_CREDIT (
ID NUMBER(10) not null,
CREDIT_CODE VARCHAR2(64 char) NULL,
COMPANY_NAME VARCHAR2(64 char) NULL,
BUSINESS_TIME DATE NULL,
ISCURRENT NUMBER(1) NULL,
LASTUPDATE DATE NULL
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
ALTER TABLE COMPANY_CREDIT ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE COMPANY_CREDIT ADD PRIMARY KEY (ID);