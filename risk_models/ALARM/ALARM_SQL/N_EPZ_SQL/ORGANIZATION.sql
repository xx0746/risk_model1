create table ORGANIZATION
(
  ID         NUMBER(10)        not null
    primary key,
  CUSTOM_ID  VARCHAR2(4 char)  not null,
  ORG_CODE   VARCHAR2(20 char) not null,
  STATUS     VARCHAR2(3 char)  not null,
  TRADE_ID   NUMBER(10)        not null,
  TYPE       VARCHAR2(4 char)  not null,
  IC_CARD    VARCHAR2(50),
  DCL_TYPECD VARCHAR2(1) default '1',
  SOURCE     VARCHAR2(10 char),
  AREA       VARCHAR2(10)
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
ALTER TABLE ORGANIZATION ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE ORGANIZATION ADD PRIMARY KEY (ID);