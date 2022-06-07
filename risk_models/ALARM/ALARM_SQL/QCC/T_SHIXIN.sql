create table T_SHIXIN (
ID NUMBER(10) not null,
COMPANY_ID VARCHAR2(64 char),
COMPANY_NAME VARCHAR2(255 char),
SX_ID VARCHAR2(64 char) NULL,
NAME VARCHAR2(255 char) NULL,
LIAN_DATE date NULL,
AN_NO VARCHAR2(64 char) NULL,
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
ALTER TABLE T_SHIXIN ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE T_SHIXIN ADD PRIMARY KEY (ID);