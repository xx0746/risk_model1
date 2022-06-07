create table PORT_RELEASE_BSC (
  ID                   NUMBER(10) not null,
  VEHICLE_NO           VARCHAR2(64 char),
  IN_EXP_TYPE          VARCHAR2(1 char),
  BIZOP_ETPS_NO        VARCHAR2(10 char),
  BIZOP_ETPS_NM        VARCHAR2(768 char),
  BIZOP_ETPS_SCCD      VARCHAR2(18 char),
  IE_TYPECD            VARCHAR2(2 char),
  PASS_TIME            date,
  AREA                 VARCHAR2(1 char)
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
ALTER TABLE PORT_RELEASE_BSC ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE PORT_RELEASE_BSC ADD PRIMARY KEY (ID);