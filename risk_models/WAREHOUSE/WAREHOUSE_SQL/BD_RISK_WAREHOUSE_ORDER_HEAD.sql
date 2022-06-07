CREATE TABLE BD_RISK_WAREHOUSE_ORDER_HEAD (
ID NUMBER(10) NOT NULL,
ORG_CODE VARCHAR2(128 CHAR) NOT NULL,
ORG_NAME VARCHAR2(128 CHAR) NOT NULL,
ORDER_NO VARCHAR2(128 CHAR) NOT NULL,
ORDER_TYPE VARCHAR2(64 CHAR) NOT NULL,
CUSTOMER_NAME VARCHAR2(128 CHAR),
CUSTOMER_COUNTRY VARCHAR2(128 CHAR),
ORDER_DATE DATE NOT NULL,
EST_DATE DATE,
COMPLETE_DATE DATE,
STATUS VARCHAR2(64 CHAR),
CAPXACTION VARCHAR2(64 CHAR) NOT NULL,
LASTUPDATEDDT DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.ORG_CODE IS '企业信用代码';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.ORG_NAME IS '企业名称';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.ORDER_NO IS '客户订单编号';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.ORDER_TYPE IS '订单类型';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.CUSTOMER_NAME IS '客户名称';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.CUSTOMER_COUNTRY IS '客户所属国别';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.ORDER_DATE IS '下单时间';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.EST_DATE IS '订单预期完成时间';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.COMPLETE_DATE IS '订单实际完成时间';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.STATUS IS '订单是否完成';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.CAPXACTION IS '增删改类型';
COMMENT ON COLUMN BD_RISK_WAREHOUSE_ORDER_HEAD.LASTUPDATEDDT IS '最后更新时间';


ALTER TABLE BD_RISK_WAREHOUSE_ORDER_HEAD ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table BD_RISK_WAREHOUSE_ORDER_HEAD
-- ----------------------------
ALTER TABLE BD_RISK_WAREHOUSE_ORDER_HEAD ADD PRIMARY KEY (ID);