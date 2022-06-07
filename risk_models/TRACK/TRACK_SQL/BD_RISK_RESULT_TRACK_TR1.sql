create table BD_RISK_RESULT_TRACK_TR1(
ID NUMBER(20) NOT NULL,
ORG_CODE VARCHAR2(64) NOT NULL ,
DOC_NUM VARCHAR2(2000) NULL,
DOC_TYPE VARCHAR2(200) NULL,
VEHICLE_NO VARCHAR2(128) NOT NULL,
BEGIN_POINT VARCHAR2(256) NULL,
BEGIN_TIME DATE NULL,
END_POINT VARCHAR2(256) NULL,
END_TIME DATE NULL,
PRED_PATH CLOB NULL,
PRED_TIME NUMBER(20,5) NULL,
REAL_PATH CLOB NULL,
REAL_TIME NUMBER(20,5) NULL,
TIME_RISK NUMBER(20,5) NULL,
TRACK_SIM NUMBER(20,5) NULL,
WAIT_LOCATION VARCHAR2(512) NULL,
TRACK_LABEL VARCHAR2(128) NULL,
TIME_LABEL VARCHAR2(128) NULL,
WAIT_LABEL VARCHAR2(128) NULL,
SCORE NUMBER(20,5) NOT NULL ,
CHECK_TIME DATE NOT NULL,
ISCURRENT NUMBER(10) NOT NULL,
LASTUPDATE DATE NOT NULL
);
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.ORG_CODE IS '经营企业单位编码';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.DOC_NUM IS '单证号';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.DOC_TYPE IS '单证类型';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.VEHICLE_NO IS '车牌号';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.BEGIN_POINT IS '出发经纬度';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.BEGIN_TIME IS '出发时间';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.END_POINT IS '到达经纬度';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.END_TIME IS '到达时间';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.PRED_PATH IS '高德预估路径';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.PRED_TIME IS '高德预估时间';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.REAL_PATH IS 'gps实际路径';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.REAL_TIME IS 'gps实际时间';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.TIME_RISK IS '时间差';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.TRACK_SIM IS '轨迹匹配率';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.WAIT_LOCATION IS '异常停靠点经纬度与时间';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.TRACK_LABEL IS '轨迹标签';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.TIME_LABEL IS '时间标签';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.WAIT_LABEL IS '停靠标签';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.SCORE IS '物流扣分';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.CHECK_TIME IS '模型运行时间';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.ISCURRENT IS '是否使用';
COMMENT ON COLUMN BD_RISK_RESULT_TRACK_TR1.LASTUPDATE IS '数据最后更新时间';

ALTER TABLE BD_RISK_RESULT_TRACK_TR1 ADD CHECK (ID IS NOT NULL);
ALTER TABLE BD_RISK_RESULT_TRACK_TR1 ADD PRIMARY KEY (ID);


