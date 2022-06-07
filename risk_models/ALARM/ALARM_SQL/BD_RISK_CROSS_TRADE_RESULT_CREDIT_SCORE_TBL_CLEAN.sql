CREATE TABLE BD_RISK_CROSS_TRADE_RESULT_CREDIT_SCORE_TBL_CLEAN (
ID NUMBER(20) NOT NULL,
CREDIT_CODE VARCHAR2(64 CHAR) NULL,
DEAL_TYPE NUMBER(10,0) NULL,
ENTRY_ID VARCHAR2(64 CHAR) NULL,
COMPANY_NAME VARCHAR2(64 CHAR) NULL,
D_DATE VARCHAR2(64 CHAR) NULL,
CUSTOM VARCHAR2(64 CHAR) NULL,
TAX_CREDIT VARCHAR2(64 CHAR) NULL,
I_E_FLAG VARCHAR2(64 CHAR) NULL,
ADMINISTRATION NUMBER(10,0) NULL,
ADMINISTRATION_3_YEARS NUMBER(10,0) NULL,
CRIMINAL_CUSTOM NUMBER(10,0) NULL,
CRIMINAL_NON_CUSTOM NUMBER(10,0) NULL,
CRIMINAL_CUSTOM_IN_FIVE NUMBER(10,0) NULL,
LOST_CREDIT_total NUMBER(10,0) NULL,
LOST_CREDIT_1st NUMBER(10,0) NULL,
LOST_CREDIT_2nd NUMBER(10,0) NULL,
LOST_CREDIT_3rd NUMBER(10,0) NULL,
EXCUTED_total NUMBER(10,0) NULL,
EXCUTED_1st NUMBER(10,0) NULL,
EXCUTED_2nd NUMBER(10,0) NULL,
EXCUTED_3rd NUMBER(10,0) NULL,
NOT_OPERATION NUMBER(10,0) NULL,
NOT_OPERATION_3_YEARS NUMBER(10,0) NULL,
SOCIAL_SECURE NUMBER(10,0) NULL,
START_FROM_NOW NUMBER(10,3) NULL,
CAL_TYPE NUMBER(1,0) NULL,
LASTUPDATE date NULL,
ISCURRENT NUMBER(1,0) NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;



ALTER TABLE BD_RISK_CROSS_TRADE_RESULT_ORIGIN_COUNTRY_DIFFER_CLEAN ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_CROSS_TRADE_RESULT_ORIGIN_COUNTRY_DIFFER_CLEAN ADD PRIMARY KEY (ID);