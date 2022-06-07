# -*- coding: utf-8 -*-
import time
import pandas as pd
import numpy as np
import datetime
from math import radians, cos, sin, asin, sqrt, ceil


class TrackDetection(object):
    def __init__(self):
        # 100米之内为正常点
        self.risk_throld = 500
        self.fix_throld = 50
        self.display_throld = 200
        # 停靠超过30分钟的点
        self.risk_wait_time = 60 * 30

    @staticmethod
    def _geodistance(point_a, point_b):
        """
        Des:计算两个经纬度的直线距离m
        """
        lat1, lng1 = point_a[0], point_a[1]
        lat2, lng2 = point_b[0], point_b[1]
        lng1, lat1, lng2, lat2 = map(radians, [float(lng1), float(lat1), float(lng2), float(lat2)])
        # 经纬度转换成弧度
        dlon = lng2 - lng1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        # 距离单位为M
        distance = 2 * asin(sqrt(a)) * 6371 * 1000  # 地球平均半径，6371km
        distance = round(distance, 3)
        return distance  # M

    def _add_point(self, point_list):
        """
        Des:补全预估路径中存在断点的经纬度信息；
        Arg:
            point_list: 
        """
        point_list = np.array(point_list)
        res = []
        for i in range(0, len(point_list) - 1):
            if i == 0:
                res.append(point_list[i])
            dis = TrackDetection._geodistance(point_list[i], point_list[i + 1])
            if dis > self.fix_throld - 1:
                add_point_num = np.ceil(dis / self.fix_throld)
                deta = (point_list[i + 1] - point_list[i]) / add_point_num
                for j in range(int(add_point_num)):
                    res.append(point_list[i] + deta * (j + 1))
            else:
                res.append(point_list[i])
        return res

    def display_add_point(self, point_list):
        """
        Des:补全预估路径中存在断点的经纬度信息；
        Arg:
            point_list: 
        """
        point_list = np.array(point_list)
        res = []
        for i in range(0, len(point_list) - 1):
            if i == 0:
                res.append(point_list[i])
            dis = TrackDetection._geodistance(point_list[i], point_list[i + 1])
            if dis > self.display_throld - 1:
                add_point_num = np.ceil(dis / self.display_throld)
                deta = (point_list[i + 1] - point_list[i]) / add_point_num
                for j in range(int(add_point_num)):
                    res.append(point_list[i] + deta * (j + 1))
            else:
                res.append(point_list[i])
        return res

    def point_geo_dis_sim(self, real_road, pred_road):
        # 基于点到预估路径， 只要点在该预估路径上则认为该点 为正常轨迹
        count = 0
        for real_point in real_road:
            for pred_point in pred_road:
                if TrackDetection._geodistance(real_point, pred_point) <= self.risk_throld:
                    count += 1
                    break
        return count / len(real_road)

    def _track_similaity(self, real_road, pred_track):
        # print('实际路径长度', len(real_road), '预估路径长度', len(pred_track))

        """
        Des:
        Arg: 
            real_road： 车辆经纬度序列（type: list）
            pred_track:  预估轨迹（typedict_keys(['0_path', '0_time', '1_path', '1_time', '2_path', '2_time'])）
        Return:
            max_id: 
            similar_list[max_id]：轨迹相似度
        """
        path_key = [i for i in pred_track.keys() if 'path' in i]
        similar_list = []
        for key in path_key:
            pred_road = pred_track[key]
            pred_road = self._add_point(pred_road)
            real_road = self._add_point(real_road)
            # print('实际路径长度', len(real_road), '预估路径长度', len(pred_track))
            similar_rate = self.point_geo_dis_sim(real_road, pred_road)
            similar_list.append(similar_rate)
        max_id = np.argmax(np.array(similar_list))
        return max_id, similar_list[max_id]

    @staticmethod
    def time_diff(data_df):
        # data_df['GPS_TIME'] = data_df['GPS_TIME'].map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        # end_time = datetime.datetime.strptime(data_df.tail(1)['GPS_TIME'], '%Y-%m-%d %H:%M:%S')
        # begin_time = datetime.datetime.strptime(data_df.head(1)['GPS_TIME'], '%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(str(data_df.tail(1)['GPS_TIME'].values[0]), '%Y-%m-%d %H:%M:%S')
        begin_time = datetime.datetime.strptime(str(data_df.head(1)['GPS_TIME'].values[0]), '%Y-%m-%d %H:%M:%S')
        return (end_time - begin_time).total_seconds()

    def find_wait_location(self, data_df):
        """
        Des: 输入 实际轨迹寻找经纬度相同的Idx序列
        Arg：
            data_df：排序及reset_index（type：pd.DataFrame; col = ['lat', 'lon', 'gpstime']）
        Return:
            result_index：返回Idx，可以根据dataFrame.loc[result_index[?]]寻找
        """
        memory_content = ''
        result_index = []
        memory_index = set()
        compute = False
        for idx, row in data_df.iterrows():

            # 遍历真实路径
            # TODO 记得切换！！！！(以前是lat lon，现在是lon，lat)
            # current_content = [row['lat'], row['lon']]
            current_content = [row['X_PARAM'], row['Y_PARAM']]
            if idx == 0:
                # 初始化
                memory_content = current_content
            else:
                # idx往后遍历的情况
                if current_content == memory_content:
                    memory_index.append(idx - 1)
                    memory_index.append(idx)

                if current_content != memory_content or idx == data_df.shape[0] - 1:
                    if len(memory_index):
                        result_index.append(memory_index)
                    memory_index = []
                memory_content = current_content
        return result_index

    @classmethod
    def track_detection(clf, real_track, pred_track):
        """
        Des: 输入实际货车轨迹与预估货车轨迹，输出 轨迹重合率、到达时间风险及异常停留位置
        Arg：
            real_track： 实际轨迹（type：pd.DataFrame; col = ['lat', 'lon', 'gpstime', 'vehicleno']）
            pred_track:  预估轨迹（typedict_keys(['0_path', '0_time', '1_path', '1_time', '2_path', '2_time'])）
        Return：
            track_sim: 轨迹重合率（百分比）（type: float）
            pred_path: 最相似的预估轨迹 [lat, lon]（np.array()）
            time_risk: 时间异常风险【超过预估时间百分比】（百分比：（实际时间 - 预估时间）/ 预估时间）（type: float）
            wait_location_time：异常停靠的路径及停靠时间（type: float）{['local point': [], wait_time: 30（分钟）}
            send_frequence_risk：实际车辆更新频率异常(低于五分钟的车辆更新频率异常)
        """
        real_track = pd.DataFrame(real_track, columns=['CAR_NO', 'X_PARAM', 'Y_PARAM', 'GPS_TIME'])
        # real_track['GPS_TIME'] = real_track['GPS_TIME'].map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        real_track.drop_duplicates(['GPS_TIME'], inplace=True)
        real_track = real_track.sort_values(by='GPS_TIME', ascending=True)
        real_track.reset_index(drop=True, inplace=True)
        # TODO 记得切换！！！！(以前是lat lon，现在是lon，lat)
        # real_path = real_track[['lat', 'lon']].values.tolist()
        real_path = real_track[['X_PARAM', 'Y_PARAM']].values.tolist()
        track_no, track_sim = clf()._track_similaity(real_path, pred_track)

        pred_path = pred_track['0_path']
        pred_time = pred_track['0_time']
        real_time = clf().time_diff(real_track)
        time_risk = (real_time - pred_time) / pred_time

        # 寻找经纬度相同的时间序列
        wait_location_idx = clf().find_wait_location(real_track)
        # TODO 记得切换！！！！(以前是lat lon，现在是lon，lat)
        # wait_location_time = [
        #     [real_track.loc[i].head(1)[['lat', 'lon']].values[0].tolist(), clf().time_diff(real_track.loc[i])] for i in
        #     wait_location_idx]
        wait_location_time = [
            [real_track.loc[i].head(1)[['X_PARAM', 'Y_PARAM']].values[0].tolist(), clf().time_diff(real_track.loc[i])] for i in
            wait_location_idx]
        wait_location_time_res = [[i[0][0], i[0][1], i[1] / 60] for i in wait_location_time if i[1] / 60 > 6]
        return str(round(track_sim * 100, 2)), real_time, str(real_path), pred_time, str(pred_path), \
               str(round(time_risk * 100, 2)), str(wait_location_time_res)
