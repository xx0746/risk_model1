import sys
import os
from os import path
import pandas as pd
import requests
import json
import math
import prcoords

sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk-model\risk_model\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
    print(path.dirname(path.dirname(os.getcwd())))
from risk_models import *
from risk_models.TRACK.TR1.track_func import *


def is_digit(x):
    x = str(x)
    try:
        x = float(x)
        return True
    except ValueError:
        return False


def get_real_track(vehicleno, start_time, end_time, version):
    """
    Des:
        获取目标车辆的起点时间 与 终点时间，返回车辆实际路径点
        PS：此处需要请求亿通平台是通的
    """
    if version == 'old':
        url = 'http://192.168.120.166:1080/custom-server/pickRide/getLocationByVehicleno'  # 旧风控版本
        payload = {"vehicleno": f"{vehicleno}", "startTime": f"{str(start_time)}", "endTime": f"{str(end_time)}"}
        headers = {"Content-Type": "application/json"}
        r = requests.post(url, data=json.dumps(payload), headers=headers)
        res = r.json()
        if res['status']:
            return res['map']['data']
        else:
            return ''
    if version == 'new':
        # 旧风控版本
        # url = 'http://192.168.120.166:1080/custom-server/pickRide/getLocationByVehicleno'
        # EP测试环境 需要把这里放到auth config
        url = 'https://api.eptrade.cn/data/gps/cfn152/v1'
        # url = 'http://192.168.120.23:8879/data/gps/cfn152/v1'
        data = {"vehicleNo": f"{vehicleno}", "startTime": f"{str(start_time)}", "endTime": f"{str(end_time)}",
                "orgCode": "31222xxxxx", "apCode": "AP_1054", "start": 1, "end": 10000}
        headers = {"Authorization": "E988FEF27B56498D99315814B6821CB1", "Content": "application/json"}
        res = requests.post(url=url, headers=headers, json=data).json()
        print(res)
        print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        if res['statusCode'] == 200:
            # print('get real track successfully')
            return res['reqData']['dataList']
        else:
            # print(res)
            return 'get real track failed'
    if version == 'DB':
        # sql_text1 = f'''
        #     select CAR_NO, X_PARAM, Y_PARAM, GPS_TIME
        #     from DW_CUS_RC.BD_RISK_GPS T1
        #     WHERE T1.CAR_NO = '{vehicleno}'
        #     and T1.GPS_TIME >= '{start_time}'
        #     and T1.GPS_TIME <= '{end_time}'
        #                                 '''
        sql_text1 = f'''
                    select CAR_NO, X_PARAM, Y_PARAM, GPS_TIME
                    from DW_CUS_RC.BD_RISK_GPS T1
                    WHERE T1.CAR_NO = '{vehicleno}'
                    and T1.GPS_TIME >= '{start_time}'
                    and T1.GPS_TIME <= '{end_time}'
                                                '''
        GPS_df = Read_Oracle().read_oracle(sql=sql_text1, database='dbalarm')
        path_array = []
        for ele in GPS_df.values:
            path_array.append(ele)
        return path_array

def get_pred_track(begin_point, end_point, car_no, method):
    """
    Des: 输入起点位置终点位置 [lon, lat]，返回预估路径 json
    EXAMPLE ：
        # 洋山海关
        yangshan = '121.886239,30.876474'
        # 中国（上海）自由贸易试验区（6号门）
        shiyanqu_6 = '121.608141,31.330975'
        begin_point = '121.886239,30.876474'
        #上飞路919号 '121.860207,31.086784'
        #end_point
        大厂 '121.432914,31.30406'
    PS:此处需要请求的高德平台是通的
    """
    # key = 'aed35d17b96a34d86719dc89f3b86f8a'
    key = '435e434ba3c0c8c9347446ce0d7183f9'
    # key = '57cc40c822231d35e77c729f15a99bd9'
    # key = 'E988FEF27B56498D99315814B6821CB1'  # 公司apikey 晓晴给的
    g = GpsRespond(begin_point, end_point, key)
    g.get_gps_road(method, car_no)
    return g.predict_road_gps


class LngLatTransfer:
    # lon:经度 lat:维度
    def __init__(self):
        self.x_pi = 3.14159265358979324 * 3000.0 / 180.0
        self.pi = math.pi  # π
        self.a = 6378245.0  # 长半轴
        self.es = 0.00669342162296594323  # 偏心率平方
        pass

    def GCJ02_to_WGS84(self, gcj_lng, gcj_lat):
        '''
        实现GCJ02坐标系向WGS84坐标系的转换
        :param gcj_lng: GCJ02坐标系下的经度
        :param gcj_lat: GCJ02坐标系下的纬度
        :return: 转换后的WGS84下经纬度
        '''
        dlat = self._transformlat(gcj_lng - 105.0, gcj_lat - 35.0)
        dlng = self._transformlng(gcj_lng - 105.0, gcj_lat - 35.0)
        radlat = gcj_lat / 180.0 * self.pi
        magic = math.sin(radlat)
        magic = 1 - self.es * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((self.a * (1 - self.es)) / (magic * sqrtmagic) * self.pi)
        dlng = (dlng * 180.0) / (self.a / sqrtmagic * math.cos(radlat) * self.pi)
        mglat = gcj_lat + dlat
        mglng = gcj_lng + dlng
        lng = gcj_lng * 2 - mglng
        lat = gcj_lat * 2 - mglat
        return lng, lat

    def _transformlat(self, lng, lat):
        ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
              0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
        ret += (20.0 * math.sin(6.0 * lng * self.pi) + 20.0 *
                math.sin(2.0 * lng * self.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lat * self.pi) + 40.0 *
                math.sin(lat / 3.0 * self.pi)) * 2.0 / 3.0
        ret += (160.0 * math.sin(lat / 12.0 * self.pi) + 320 *
                math.sin(lat * self.pi / 30.0)) * 2.0 / 3.0
        return ret

    def _transformlng(self, lng, lat):
        ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
              0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
        ret += (20.0 * math.sin(6.0 * lng * self.pi) + 20.0 *
                math.sin(2.0 * lng * self.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lng * self.pi) + 40.0 *
                math.sin(lng / 3.0 * self.pi)) * 2.0 / 3.0
        ret += (150.0 * math.sin(lng / 12.0 * self.pi) + 300.0 *
                math.sin(lng / 30.0 * self.pi)) * 2.0 / 3.0
        return ret


class GpsRespond(object):
    def __init__(self, begin_point, end_point, key):
        b = begin_point.split(',')[0] + '%2C' + begin_point.split(',')[1]
        e = end_point.split(',')[0] + '%2C' + end_point.split(',')[1]
        print(b)
        print(e)
        print("**********************")
        # TODO 生产更换高德api端口 ，原接口地址为
        self.requests_url = f'https://restapi.amap.com/v4/direction/truck?cartype=&' \
         f'destination={e}&height=&key={key}&load=12&number=&origin={b}&province=沪&size=4&strategy=10&width='
        # self.requests_url = \
        #     f'http://192.168.120.114:9002/v4/direction/truck?cartype=&' \
        #     f'destination={e}&height=&key={key}&load=12&number=&origin={b}&province=沪&size=4&strategy=1&width='
        self.respond_content = ''
        self.predict_road_num = ''
        self.predict_road_gps = dict()

    def get_gps_road(self, method, carNo):
        """获取预估的GPS路径
        ：param requests_url 请求URL
        ：Return road gps point : gps点列表 [[lat, lon]]
        """
        # 这里被注释掉了，晚点需要改掉
        if method == "URL":
            self.respond_content = requests.get(self.requests_url).json()
            # with open('../BDRISK_TRACK/api_res/res7.json', 'r', encoding='utf-8') as fp:
            #     self.respond_content = json.load(fp)
            print(self.respond_content)
            self.predict_road_num = self.respond_content['data']['count']
            # print(f"prediction gps road num is {self.predict_road_num}")
            for road_num in range(0, int(self.predict_road_num)):
                self.predict_road_gps[str(road_num) + '_path'] = self._parse_gps_point(road_num)
                self.predict_road_gps[str(road_num) + '_time'] = self.respond_content['data']['route']['paths'][road_num]['duration']
        else:
            sql_text1 = f'''
                    select CAR_NO, X_PARAM, Y_PARAM, DURATION
                    from DW_CUS_RC.BD_RISK_GAODE_GPS T1
                    WHERE T1.CAR_NO = '{carNo}'
                                '''
            GPS_df = Read_Oracle().read_oracle(sql=sql_text1, database='dbalarm')
            print(GPS_df)
            path_array = []
            for ele in GPS_df.values:
                print(ele)
                path_array.append([ele[1], ele[2]])
            self.predict_road_gps['0_path'] = path_array
            self.predict_road_gps['0_time'] = GPS_df.values[0][3]
        return self.get_gps_road

    def _parse_gps_point(self, road_num):
        pline = []
        for steps in self.respond_content['data']['route']['paths'][road_num]['steps']:
            if len(steps['polyline'].split(';')) >= 1:
                for j in steps['polyline'].split(';'):
                    lon, lat = j.split(',')
                    lon, lat = float(lon), float(lat)
                    # 这里是原本返回的!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    # TODO 记得切换！！！！(以前是lat lon，现在是lon，lat)
                    # pline.append([lat, lon])
                    #         修改返回后的lat long 顺序
                    pline.append([lon, lat])
            else:
                pline.append(steps['polyline'])
        print("HAHAHAHAHAHAHAHAHAHAHAHAHAHAHA")
        print(pline)
        print("----------------------------------------")
        pline = self._gps_gcj02_to_wgs84(pline)
        print(pline)
        print("|||||||||||||||||||||||||||||||||||||||||||||||||||||")
        return pline

    @staticmethod
    def _gps_gcj02_to_wgs842(pline):
        change_pline = []
        Instance = LngLatTransfer()
        for i in pline:
            # 把pline里的经纬度从GCJ02坐标系向WGS84坐标系的转换
            lon, lat = i[0], i[1]
            result = Instance.GCJ02_to_WGS84(lat, lon)
            change_pline.append([round(result[1], 6), round(result[0], 6)])
        return change_pline

    @staticmethod
    def _gps_gcj02_to_wgs84(pline):
        change_pline = []
        #         Instance = LngLatTransfer()
        for i in pline:
            # 把pline里的经纬度从GCJ02坐标系向WGS84坐标系的转换
            a = prcoords.Coords(i[1], i[0])
            b = prcoords.gcj_wgs(a)
            change_pline.append([round(b.lon, 6), round(b.lat, 6)])
        return change_pline


def match_area(area_data_df, info_df):
    is_find = 0
    min_i = -1
    match_index = -1
    min_distance = -1
    for area_index in range(0, len(info_df)):
        if is_find == 1:
            break
        else:
            for i in range(0, len(area_data_df)):
                distance = TrackDetection._geodistance(info_df.iloc[area_index][['X_PARAM', 'Y_PARAM']].values.tolist(), area_data_df.iloc[i][['X_PARAM', 'Y_PARAM']].values.tolist())
                if distance <= 500:
                    is_find = 1
                    if min_distance == -1:
                        min_i = i
                        min_distance = distance
                        match_index = area_index
                    elif distance < min_distance:
                        min_i = i
                        min_distance = distance
                        match_index = area_index
    print(area_data_df.iloc[min_i])
    print(info_df.iloc[match_index])

    return match_index, min_i

def get_area_data_df():
    # ["洋山综保区", "洋山特殊综合保税区2号卡口（保税）", 121.877742, 30.873711],
    # ["洋山综保区", "洋山特殊综合保税区5号卡口（保税）", 121.853676, 31.082446],
    data_array = [["港区", "洋山二期码头出卡口", 122.058972, 30.638727],
                  ["港区", "外一码头卡口", 121.588282, 31.363111],
                  ["港区", "外二码头卡口A", 121.570926, 31.365734],
                  ["港区", "外四码头卡口", 121.642465, 31.328418],
                  ["港区", "外五码头卡口B", 121.657044, 31.320920],
                  ["港区", "外六码头卡口", 121.670038, 31.313119],
                  ["各特殊监管区域", "外高桥保税区6号卡口", 121.608206, 31.330864],
                  ["各特殊监管区域", "外高桥保税区7号卡口", 121.618951, 31.304128],
                  ["各特殊监管区域", "外高桥港综保区卡口", 121.557249, 31.369966],
                  ["各特殊监管区域", "浦东机场综合保税区", 121.780227, 31.132792],
                  ["各特殊监管区域", "松江综合保税区A区", 121.291408, 31.008591],
                  ["各特殊监管区域", "松江综合保税区B区", 121.170141, 31.001350],
                  ["各特殊监管区域", "嘉定综合保税区", 121.169036, 31.397481],
                  ["各特殊监管区域", "青浦综合保税区", 121.151654, 31.187365],
                  ["各特殊监管区域", "金桥综合保税区", 121.677806, 31.223364],
                  ["各特殊监管区域", "漕河泾综合保税区", 121.504971, 31.103144],
                  ["各特殊监管区域", "奉贤综合保税区", 121.432774, 30.952771]
                  ]
    return pd.DataFrame(data_array, columns=['AREA', 'DISTRICT', 'X_PARAM', 'Y_PARAM'])

def get_port_area_df():
    data_array = [["港区", "洋山二期码头出卡口", 122.058972, 30.638727],
                  ["港区", "外一码头卡口", 121.588282, 31.363111],
                  ["港区", "外二码头卡口A", 121.570926, 31.365734],
                  ["港区", "外四码头卡口", 121.642465, 31.328418],
                  ["港区", "外五码头卡口B", 121.657044, 31.320920],
                  ["港区", "外六码头卡口", 121.670038, 31.313119]
                  ]
    return pd.DataFrame(data_array, columns=['AREA', 'DISTRICT', 'X_PARAM', 'Y_PARAM'])


def get_sp_area_df():
    data_array = [
                  ["各特殊监管区域", "外高桥保税区6号卡口", 121.608206, 31.330864],
                  ["各特殊监管区域", "外高桥保税区7号卡口", 121.618951, 31.304128],
                  ["各特殊监管区域", "外高桥港综保区卡口", 121.557249, 31.369966],
                  ["各特殊监管区域", "浦东机场综合保税区", 121.780227, 31.132792],
                  ["各特殊监管区域", "松江综合保税区A区", 121.291408, 31.008591],
                  ["各特殊监管区域", "松江综合保税区B区", 121.170141, 31.001350],
                  ["各特殊监管区域", "嘉定综合保税区", 121.169036, 31.397481],
                  ["各特殊监管区域", "青浦综合保税区", 121.151654, 31.187365],
                  ["各特殊监管区域", "金桥综合保税区", 121.677806, 31.223364],
                  ["各特殊监管区域", "漕河泾综合保税区", 121.504971, 31.103144],
                  ["各特殊监管区域", "奉贤综合保税区", 121.432774, 30.952771]
                  ]
    return pd.DataFrame(data_array, columns=['AREA', 'DISTRICT', 'X_PARAM', 'Y_PARAM'])



if __name__ == '__main__':
    print("--------------------------------------------------------")
    area_data_df = get_area_data_df()
    '''
    AREA               港区
DISTRICT    洋山二期码头出卡口
X_PARAM       122.059
Y_PARAM       30.6387
Name: 2, dtype: object
CAR_NO                  沪ED6696
X_PARAM                  122.06
Y_PARAM                 30.6367
GPS_TIME    2022-05-23 10:34:02
Name: 97, dtype: object
DW_CUS_RC/easipass
192.168.130.225:1521/pdbcusdev
122.06043%2C 30.6367
121.853676%2C 31.082446
passtime = 2022-05-23 13:04:37
info_list = get_real_track(vehicleno="沪ED6696", start_time="2022-05-21 11:04:37",
end_time="2022-05-23 13:04:37", version="new")
    '''

    '''
AREA             港区
DISTRICT     外六码头卡口
X_PARAM      121.67
Y_PARAM     31.3131
Name: 5, dtype: object
CAR_NO                  沪FH6653
X_PARAM                 121.668
Y_PARAM                 31.3071
GPS_TIME    2022-05-23 15:23:10
Name: 2545, dtype: object
DW_CUS_RC/easipass
192.168.130.225:1521/pdbcusdev
2022-05-23 16:20:02

info_list = get_real_track(vehicleno="沪FH6653", start_time="2022-05-21 20:04:37",
end_time="2022-05-23 20:04:37", version="new")
    '''

    # info_list = get_real_track(vehicleno="沪FH6006", start_time="2022-05-21 20:04:37",
    #                            end_time="2022-05-23 20:04:37", version="new")
    # 空数据
    # info_list = get_real_track(vehicleno="沪EF1373", start_time="2022-05-20 16:04:37",
    #                            end_time="2022-05-22 20:04:37", version="new")
    #空数据

    zongbao_location = "121.877742,30.873711"
    location_array = ["122.058972,30.638727", #洋山港- 综保区
                      "121.557249,31.369966", #外高桥港-综保区
                      "121.608206,31.330864", #外高桥保税区-综保区
                      "121.291408,31.008591"] #综保区-松江综保区
    car_no_array = ["沪ED6696",
                    "沪FH6653", "沪FH6006",
                    "沪EF1373",
                    "沪EH9256"]
    car_no = car_no_array[2]


    # info_list = get_real_track(vehicleno="沪FH6006", start_time="2022-05-21 16:20:02",
    #                            end_time="2022-05-23 16:20:02", version="new")
    # print(pd.DataFrame(info_list))
    # info_df = pd.DataFrame(info_list)
    # info_df = info_df[['vehicleNo', 'gpsTime', 'lon', 'lat']]
    # info_df.columns = ['CAR_NO', 'GPS_TIME', 'X_PARAM', 'Y_PARAM']
    # info_df = info_df.sort_values(by='GPS_TIME', ascending=True)
    # info_df['ID'] = 0
    # if info_df is not None:
    #     Write_Oracle_Alarm().write_oracle('BD_RISK_GPS', info_df, org_code=None, alarm=None)
    # print(info_df)
    # print(TrackDetection.time_diff(info_df))

    # 存储高德GPS数据方法
    predict_road_gps_res = get_pred_track(begin_point=location_array[1], end_point=zongbao_location, car_no=car_no, method="URL")
    path_array = GpsRespond._gps_gcj02_to_wgs84(predict_road_gps_res['0_path'])
    res_df_gaode = pd.DataFrame(path_array, columns=['X_PARAM', 'Y_PARAM'])
    res_df_gaode['CAR_NO'] = car_no
    res_df_gaode['ID'] = 0
    res_df_gaode['DURATION'] = predict_road_gps_res['0_time']
    print(res_df_gaode)
    if res_df_gaode is not None:
        Write_Oracle_Alarm().write_oracle('BD_RISK_GAODE_GPS', res_df_gaode, org_code=None, alarm=None)

    # 存储GPS数据方法
    # delta_seconds = datetime.timedelta(seconds=2)
    # time_now = datetime.datetime.now()
    # for i in range(0, len(path_array)):
    #     path_array[i].append(time_now + i * delta_seconds * 2)
    # res_df = pd.DataFrame(path_array, columns=['X_PARAM', 'Y_PARAM', 'GPS_TIME'])
    # res_df['GPS_TIME'] = res_df['GPS_TIME'].map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    # res_df['CAR_NO'] = car_no
    # res_df['ID'] = 0
    # print(res_df)
    # if res_df is not None:
    #     Write_Oracle_Alarm().write_oracle('BD_RISK_GPS', res_df, org_code=None, alarm=None)




