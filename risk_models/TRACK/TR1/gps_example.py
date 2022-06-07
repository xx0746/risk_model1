import prcoords
class GpsRespond(object):
    def __init__(self, begin_point, end_point, key):
        b = begin_point.split(',')[0] + '%2C' + begin_point.split(',')[1]
        e = end_point.split(',')[0] + '%2C' + end_point.split(',')[1]
        # TODO 生产更换高德api端口 ，原接口地址为
        #  f'https://restapi.amap.com/v4/direction/truck?cartype=&' \
        #  f'destination={e}&height=&key={key}&load=12&number=&origin={b}&province=沪&size=4&strategy=1&width='
        self.requests_url = \
            f'https://restapi.amap.com/v4/direction/truck?cartype=&' \
            f'destination={e}&height=&key={key}&load=12&number=&origin={b}&province=沪&size=4&width='
        self.respond_content = ''
        self.predict_road_num = ''
        self.predict_road_gps = dict()

    def get_gps_road(self):
        """获取预估的GPS路径
        ：param requests_url 请求URL
        ：Return road gps point : gps点列表 [[lat, lon]]
        """
        # 这里被注释掉了，晚点需要改掉
        self.respond_content = requests.get(self.requests_url).json()
        # with open('../BDRISK_TRACK/api_res/res7.json', 'r', encoding='utf-8') as fp:
        #     self.respond_content = json.load(fp)
        self.predict_road_num = self.respond_content['data']['count']
        # print(f"prediction gps road num is {self.predict_road_num}")
        for road_num in range(0, int(self.predict_road_num)):
            self.predict_road_gps[str(road_num) + '_path'] = self._parse_gps_point(road_num)
            self.predict_road_gps[str(road_num) + '_time'] = self.respond_content['data']['route']['paths'][road_num][
                'duration']
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

        pline = self._gps_gcj02_to_wgs84(pline)
        return pline

    @staticmethod
    def _gps_gcj02_to_wgs84(pline):
        change_pline = []
#         Instance = LngLatTransfer()
        for i in pline:
            # 把pline里的经纬度从GCJ02坐标系向WGS84坐标系的转换
            a = prcoords.Coords(i[1],i[0])
#             lon, lat = i[0], i[1]
#             result = Instance.GCJ02_to_WGS84(lat, lon)
            b = prcoords.gcj_wgs(a)
            change_pline.append([round(b.lon, 6), round(b.lat, 6)])
        return change_pline

    def get_pred_track(begin_point, end_point):
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
        # key = '435e434ba3c0c8c9347446ce0d7183f9'
        key = '57cc40c822231d35e77c729f15a99bd9'  # 公司apikey
        g = GpsRespond(begin_point, end_point, key)
        g.get_gps_road()
        return g.predict_road_gps