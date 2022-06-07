CREATE TABLE WAREHOUSE_RECEIPT_BSC (
ID NUMBER(20) NOT NULL ,
ORG_ID VARCHAR2(64 CHAR) NULL,
WH_REC_PREENT_NO VARCHAR2(64 CHAR) NULL,
BIZOP_ETPSNO VARCHAR2(64 CHAR) NULL,
BIZOP_ETPS_NM VARCHAR2(64 CHAR) NULL,
BIZOP_ETPS_SCCD VARCHAR2(64 CHAR) NULL,
BUSINESS_TYPECD VARCHAR2(64 CHAR) NULL,
RLT_ENTRY_NO VARCHAR2(64 CHAR) NULL,
RLT_WH_REC_NO VARCHAR2(64 CHAR) NULL,
WH_REC_NO VARCHAR2(64 CHAR) NULL,
OPT_STATUS VARCHAR2(64 CHAR) NULL,
IN_EXP_TYPE VARCHAR2(64 CHAR) NULL,
IE_TYPECD VARCHAR2(64 CHAR) NULL,
MASTER_CUSCD VARCHAR2(64 CHAR) NULL,
DCL_TIME date NULL,
TYPE VARCHAR2(64 CHAR) NULL,
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
ALTER TABLE WAREHOUSE_RECEIPT_BSC ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE WAREHOUSE_RECEIPT_BSC ADD PRIMARY KEY (ID);