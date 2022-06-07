create table COMPANY_REL (
ID NUMBER(10) not null,
LICENSE_ID VARCHAR2(64 char) NULL,
TRADE_CO VARCHAR2(64 char) NULL,
CO_CLASS VARCHAR2(64 char) NULL,
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
ALTER TABLE COMPANY_REL ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE COMPANY_REL ADD PRIMARY KEY (ID);