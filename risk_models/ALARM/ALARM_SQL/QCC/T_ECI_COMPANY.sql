create table T_ECI_COMPANY (
ID NUMBER(10) not null,
COMPANY_ID VARCHAR2(64 char),
COMPANY_NAME VARCHAR2(64 char),
START_DATE DATE,
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
ALTER TABLE T_ECI_COMPANY ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE T_ECI_COMPANY ADD PRIMARY KEY (ID);