import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk_model\risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_RESULT_TRACK_TR1, _name_BD_RISK_TRACK_INFO
from risk_models.TRACK.TR1.track_utils import *
from risk_models.TRACK.TR1.track_func import *


class GpsModel(object):
    def __init__(self, org_code, params, base_time, child_task_id, info):
        '''
        VEHICLE_NO as 车牌号,
        IN_EXP_TYPE as 出入库类型,
        IE_TYPECD as 进出标志,
        BIZOP_ETPS_NO as 海关10位编码,
        BIZOP_ETPS_NM as 企业名称,
        BIZOP_ETPS_SCCD as 企业信用代码,
        PASS_TIME as 过卡时间1,
        AREA（区域标识,：L-芦潮港；J-机场南侧；D-大场区域）
        '''
        self.model_code = 'TRACK'
        self.child_model_code = 'TR1'
        self.child_task_id = child_task_id
        self.car_No = info["VEHICLE_NO"]
        self.I_E_Type = info["IE_TYPECD"]
        self.pass_time = info["PASS_TIME"]
        self.business_type = info["IN_EXP_TYPE"]
        self.area = info["AREA"]
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = info["BIZOP_ETPS_SCCD"]
        self.track_tar = json.loads(params)['track_sim']
        self.time_tar = json.loads(params)['track_time']
        self.wait_time_tar = json.loads(params)['wait_time']
        self.track_score = json.loads(params)['track_score']
        self.time_score = json.loads(params)['time_score']
        self.wait_score = json.loads(params)['wait_score']

        self.area_data_df = get_area_data_df()

    def get_match_start(self, end_time):
        # two_delta_days = datetime.timedelta(days=1)
        # start_time = (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - two_delta_days).strftime("%Y-%m-%d %H:%M:%S")
        # info_list = get_real_track(vehicleno=self.car_No, start_time=start_time,
        #                            end_time=end_time, version="DB")
        # info_df = pd.DataFrame(info_list, columns=['CAR_NO', 'X_PARAM', 'Y_PARAM', 'GPS_TIME'])
        # info_df = info_df.sort_values(by='GPS_TIME', ascending=False) #匹配起点，小于过卡时间，最晚点，降序
        # match_index, min_i = match_area(self.area_data_df, info_df)
        # start_time = info_df.iloc[match_index]['GPS_TIME']
        # start_point = info_df.iloc[match_index][['X_PARAM', 'Y_PARAM']].values.tolist()
        start_point = []
        start_time = ""
        zongbao_location = "121.877742,30.873711"
        location_array = ["122.058972,30.638727",  # 洋山港- 综保区
                          "121.557249,31.369966",  # 外高桥港-综保区
                          "121.608206,31.330864",  # 外高桥保税区-综保区
                          "121.291408,31.008591"]  # 综保区-松江综保区
        car_no_array = ["沪ED6696",
                        "沪FH6653", "沪FH6006",
                        "沪EF1373",
                        "沪EH9256"]
        if self.car_No == "沪ED6696":
            start_point = [122.058972, 30.638727]
            start_time = "2022-05-26 10:56:58"
        elif self.car_No == "沪FH6653":
            start_point = [121.557249, 31.369966]
            start_time = "2022-05-26 10:56:58"
        elif self.car_No == "沪FH6006":
            start_point = [121.557249, 31.369966]
            start_time = "2022-05-26 10:56:58"
        elif self.car_No == "沪EF1373":
            start_point = [121.608206, 31.330864]
            start_time = "2022-05-26 10:56:58"


        return start_point, start_time


    def get_match_end(self, start_time):
        # info_list = get_real_track(vehicleno=self.car_No, start_time=start_time,
        #                            end_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), version="DB")
        # info_df = pd.DataFrame(info_list, columns=['CAR_NO', 'X_PARAM', 'Y_PARAM', 'GPS_TIME'])
        # info_df = info_df.sort_values(by='GPS_TIME', ascending=True) #匹配终点，大于过卡时间，最早点，升序
        # match_index, min_i = match_area(self.area_data_df, info_df)
        # end_time = info_df.iloc[match_index]['GPS_TIME']
        # end_point = self.area_data_df.iloc[min_i][['X_PARAM', 'Y_PARAM']].values.tolist()
        end_point = [121.291408, 31.008591]
        end_time = "2022-05-26 14:56:58"
        if self.car_No == "沪EH9256":
            end_point = [121.291408, 31.008591]
            end_time = "2022-05-26 14:56:58"
        return end_point, end_time

    def parse_target(self):
        id = 0
        org_code = self.org_code
        carnum = self.car_No
        """
        先判断进出
          如果为出，卡口数据为开始坐标，时间，匹配终点坐标时间
          如果为进，卡口数据为结束坐标，时间，匹配开始坐标时间
        判断区域
            J用5好卡口
            其余用2号卡口
        """
        if self.I_E_Type == "E":
            if self.area == "J":
                start_point = [121.853676, 31.082446]
            else:
                start_point = [121.877742, 30.873711]
            start_time = self.pass_time.strftime("%Y-%m-%d %H:%M:%S")
            end_point, end_time = self.get_match_end(start_time)
        else:

            if self.area == "J":
                end_point = [121.853676, 31.082446]
            else:
                end_point = [121.877742, 30.873711]
            end_time = self.pass_time.strftime("%Y-%m-%d %H:%M:%S")
            start_point, start_time = self.get_match_start(end_time)

        return id, org_code, carnum, start_time, end_time, start_point, end_point

    def transfer_point_pointStr(self, point):
        pointStr = str(point)
        pointStr = pointStr.replace('[', "")
        pointStr = pointStr.replace(']', "")
        return pointStr

    def model_tr1(self):
        res=[]
        id, org_code, carnum, start_time, end_time, start_point, end_point = self.parse_target()
        start_pointStr = self.transfer_point_pointStr(start_point)
        end_pointStr = self.transfer_point_pointStr(end_point)
        real_track = get_real_track(carnum, start_time, end_time, version='DB')
        pred_track = get_pred_track(begin_point=start_pointStr, end_point=end_pointStr, car_no=carnum)
        tracksim, realtime, realpath, predtime, predpath, timerisk, waitlocation = \
            TrackDetection.track_detection(real_track, pred_track)
        df = {}
        df['ORG_CODE'] = org_code
        df['VEHICLE_NO'] = carnum
        # TODO 需要切换 0 和 1 的位置
        # 注意这里的begin——point 和 end——point都是反的 31，121
        df['BEGIN_POINT'] = [float(start_pointStr.split(',')[0]), float(start_pointStr.split(',')[1])]
        df['BEGIN_TIME'] = start_time
        df['END_POINT'] = [float(end_pointStr.split(',')[0]), float(end_pointStr.split(',')[1])]
        df['END_TIME'] = end_time
        df['REAL_PATH'] = realpath
        df['PRED_PATH'] = predpath
        df['REAL_TIME'] = realtime
        df['PRED_TIME'] = predtime
        df['TIME_RISK'] = timerisk
        df['TRACK_SIM'] = tracksim
        df['WAIT_LOCATION'] = waitlocation
        df = pd.DataFrame([df])
        df['CHECK_TIME'] = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
        df['CHECK_TIME'] = pd.to_datetime(df['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        df['TRACK_LABEL'] = df['TRACK_SIM'].map(
            lambda x: '轨迹异常，重合率为: %.2f %%' % float(x) if is_digit(x) and float(x) < self.track_tar
            else '轨迹重合率为: {}%'.format(x))
        df['TRACK_FLAG'] = df['TRACK_SIM'].map(lambda x: '1' if is_digit(x) and float(x) < self.track_tar
            else '0')
        df['TIME_FLAG'] = df['TRACK_SIM'].map(lambda x: '1' if is_digit(x) and abs(float(x)) >= self.time_tar
            else '0')
        df['TIME_LABEL'] = df['TIME_RISK'].map(
            lambda x: '行驶时间异常，相差百分比为: %.2f %%' % float(x) if is_digit(x) and abs(float(x)) >= self.time_tar
            else '行驶时间正常，相差百分比为: {}%'.format(x))
        df['WAIT_LABEL'] = df['WAIT_LOCATION'].apply(
            lambda x: '无异常停留' if json.loads(x) == []
            else f'发现异常停靠点{len(json.loads(x))}处')
        for i in ['BEGIN_POINT', 'END_POINT', 'PRED_PATH', 'REAL_PATH', 'WAIT_LOCATION', 'TRACK_LABEL']:
            df[i] = df[i].map(str)
        for i in ['TIME_RISK', 'REAL_TIME', 'TRACK_SIM']:
            df[i] = df[i].map(float)
        df['BEGIN_POINT'] = df['BEGIN_POINT'].apply(lambda x: x.replace("'", ''))
        df['END_POINT'] = df['END_POINT'].apply(lambda x: x.replace("'", ''))
        df['BEGIN_TIME'] = df['BEGIN_TIME'].astype('datetime64')
        df['END_TIME'] = df['END_TIME'].astype('datetime64')
        df['SCORE'] = df['TRACK_LABEL'].apply(lambda x: self.track_score if '异常' in x else 0) + \
                      df['TIME_LABEL'].apply(lambda x: self.time_score if '异常' in x else 0) + \
                      df['WAIT_LABEL'].apply(lambda x: self.wait_score if '发现' in x else 0)
        df = df.reset_index().rename(columns={'index': 'ID'})
        res.append(df.to_dict('records')[0])
        Write_Oracle_Alarm().write_oracle(_name_BD_RISK_RESULT_TRACK_TR1, df, org_code=None, alarm=None)

    def run_model_tr1(self):
        exec_status = None
        try:
            self.model_tr1()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id=self.child_task_id, exec_status=exec_status).write_log()



if __name__ == '__main__':
    if params_global.is_test:
        child_task_id = 'childtaskidtr001'
    else:
        child_task_id = sys.argv[1]
    param_json = '{"track_sim":25,"track_time":80, "wait_time":20, "track_score":-0.8,"time_score":-0.5,"wait_score":-0.8}'

    org_code = "123456123456123456"
    base_time = "2022-05-22"

    sql_text = '''
        SELECT 
        VEHICLE_NO,
        IN_EXP_TYPE,
        IE_TYPECD,
        BIZOP_ETPS_NO,
        BIZOP_ETPS_NM,
        BIZOP_ETPS_SCCD,
        PASS_TIME,
        AREA
        FROM DW_CUS_RC.PORT_RELEASE_BSC 
        where IN_EXP_TYPE in ('1','5')
        and PORT_IOCHKPT_STUCD = '2'
        and ISCURRENT = 1
                '''
    car_info_df = Read_Oracle().read_oracle(sql=sql_text, database='dbalarm')
    for i in range(0, len(car_info_df)):
        GpsModel(org_code="", params=param_json, base_time=base_time, child_task_id=child_task_id, info=car_info_df.iloc[i]).run_model_tr1()
