import sys, os
from os import path
from datetime import date,timedelta

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"F:/bdrisk_model/model_20220531/bdrisk-model-dev/risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))

sys.path.append("../../../")
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))


from risk_models import *
from risk_models import _name_BD_RISK_RESULT_TRACK_TR1, _name_BD_RISK_TRACK_INFO
from risk_models.TRACK.TR1.track_utils import *
from risk_models.TRACK.TR1.track_func import *
import gaode_path_svc

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
        self.preent_no = info['PREENT_NO']
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = info["BIZOP_ETPS_SCCD"]
        self.track_tar = json.loads(params)['track_sim']
        self.time_tar = json.loads(params)['track_time']
        self.wait_time_tar = json.loads(params)['wait_time']
        self.track_score = json.loads(params)['track_score']
        self.time_score = json.loads(params)['time_score']
        self.wait_score = json.loads(params)['wait_score']

        
        self.port_area_df = get_port_area_df()
        self.sp_area_df = get_sp_area_df()


    def get_match_start(self, end_time,area_data_df):
        two_delta_days = datetime.timedelta(days=2)
        start_time = (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - two_delta_days).strftime("%Y-%m-%d %H:%M:%S")
        info_list = get_real_track(vehicleno=self.car_No, start_time=start_time,
                                   end_time=end_time, version="DB")

        if len(info_list)==0:
            return -1,-1,""

        info_df = pd.DataFrame(info_list, columns=['CAR_NO', 'X_PARAM', 'Y_PARAM', 'GPS_TIME'])
        info_df = info_df.sort_values(by='GPS_TIME', ascending=False) #匹配起点，小于过卡时间，最晚点，降序
        match_index, min_i = match_area(area_data_df, info_df)
        if match_index == -1 or min_i == -1:
            print("没有匹配到")
            return -1, -1,""
        else:
            start_time = info_df.iloc[match_index]['GPS_TIME']
            start_point = info_df.iloc[match_index][['X_PARAM', 'Y_PARAM']].values.tolist()
            start_point_name = area_data_df.iloc[min_i]['DISTRICT']
            return start_point, start_time,start_point_name
            

    def get_match_end(self, start_time,area_data_df):
        info_list = get_real_track(vehicleno=self.car_No, start_time=start_time,end_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), version="DB")
        if len(info_list)==0:
            return -1, -1,""
        info_df = pd.DataFrame(info_list, columns=['CAR_NO', 'X_PARAM', 'Y_PARAM', 'GPS_TIME'])
        info_df = info_df.sort_values(by='GPS_TIME', ascending=True) #匹配终点，大于过卡时间，最早点，升序
        match_index, min_i = match_area(area_data_df, info_df)
        if match_index == -1 or min_i == -1:
            return -1, -1,""
        else:
            end_time = info_df.iloc[match_index]['GPS_TIME']
            end_point = area_data_df.iloc[min_i][['X_PARAM', 'Y_PARAM']].values.tolist()
            end_point_name = area_data_df.iloc[min_i]['DISTRICT']
            return end_point, end_time,end_point_name
            

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
        # 设置搜索区域， self.business_type =1 ，搜索港区，
        # self.business_type=5 搜索特殊区域
        if self.business_type == '1':
            search_area = self.port_area_df
        else:
            search_area = self.sp_area_df

        if self.I_E_Type == "E":
            if self.area == "J":
                start_point = [121.853676, 31.082446]
                start_point_name = "洋山特殊综合保税区5号卡口（保税）"
            else:
                start_point = [121.877742, 30.873711]
                start_point_name = "洋山特殊综合保税区2号卡口（保税）"
            start_time = self.pass_time.strftime("%Y-%m-%d %H:%M:%S")
            
            end_point, end_time, end_point_name = self.get_match_end(start_time,search_area)
        else:

            if self.area == "J":
                end_point = [121.853676, 31.082446]
                end_point_name = "洋山特殊综合保税区5号卡口（保税）"
            else:
                end_point = [121.877742, 30.873711]
                end_point_name = "洋山特殊综合保税区2号卡口（保税）"
            end_time = self.pass_time.strftime("%Y-%m-%d %H:%M:%S")
            #start_point, start_time,start_point_name = self.get_match_start(end_time)
            start_point, start_time,start_point_name = self.get_match_start(end_time,search_area)

        return id, org_code, carnum, start_time, end_time, start_point, end_point,start_point_name,end_point_name

    def transfer_point_pointStr(self, point):
        pointStr = str(point)
        pointStr = pointStr.replace('[', "")
        pointStr = pointStr.replace(']', "")
        return pointStr

    def model_tr1(self):
        res=[]
        start_point_name =""
        end_point_name = ""
        id, org_code, carnum, start_time, end_time, start_point, end_point,start_point_name,end_point_name = self.parse_target()
        
        if start_time == -1 or end_time == -1 or start_point == -1 or end_point == -1:
            return
        start_pointStr = self.transfer_point_pointStr(start_point)
        end_pointStr = self.transfer_point_pointStr(end_point)
        real_track = get_real_track(carnum, start_time, end_time, version='DB')
        #pred_track = get_pred_track(begin_point=start_pointStr, end_point=end_pointStr, car_no=carnum, method="DB")
        pred_tracks = gaode_path_svc.get_gps_coordinates(start_point_name,end_point_name)
        print("size of route:",len(pred_tracks))
        
        if len(pred_tracks)==0:
            return

        max_tracksim = -1.0

        for pred_track in pred_tracks:

            tracksim, realtime, realpath, predtime, predpath, timerisk, waitlocation = \
                TrackDetection.track_detection(real_track, pred_track)

            print("track_sim:=====>",tracksim)
            if float(tracksim) > max_tracksim:
                max_tracksim = float(tracksim)
                
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
                df['TIME_RISK'] = timerisk.replace('-','')
                df['TRACK_SIM'] = tracksim
                #df['WAIT_LOCATION'] = waitlocation
                df['WAIT_LOCATION'] = ' '
                df = pd.DataFrame([df])
                df['CHECK_TIME'] = datetime.datetime.strftime(datetime.datetime.now(), format='%Y-%m-%d %H:%M:%S')
                df['CHECK_TIME'] = pd.to_datetime(df['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
                df['TRACK_LABEL'] = df['TRACK_SIM'].map(
                    lambda x: '轨迹异常，重合率为: %.2f %%' % float(x) if is_digit(x) and float(x) < self.track_tar
                    else '轨迹重合率为: {}%'.format(x))
                df['TRACK_FLAG'] = df['TRACK_SIM'].map(lambda x: '1' if is_digit(x) and float(x) < self.track_tar
                    else '0')
                df['TIME_FLAG'] = df['TIME_RISK'].map(lambda x: '1' if is_digit(x) and abs(float(x)) >= self.time_tar
                    else '0')
                df['TIME_LABEL'] = df['TIME_RISK'].map(
                    lambda x: '行驶时间异常，相差百分比为: %.2f %%' % float(x) if is_digit(x) and abs(float(x)) >= self.time_tar
                    else '行驶时间正常，相差百分比为: {}%'.format(x))
                #df['WAIT_LABEL'] = df['WAIT_LOCATION'].apply(
                #    lambda x: '无异常停留' if json.loads(x) == []
                #    else f'发现异常停靠点{len(json.loads(x))}处')
                df['WAIT_LABEL'] = ' '
                for i in ['BEGIN_POINT', 'END_POINT', 'PRED_PATH', 'REAL_PATH', 'WAIT_LOCATION', 'TRACK_LABEL']:
                    df[i] = df[i].map(str)
                for i in ['TIME_RISK', 'REAL_TIME', 'TRACK_SIM']:
                    df[i] = df[i].map(float)
                df['BEGIN_POINT'] = df['BEGIN_POINT'].apply(lambda x: x.replace("'", ''))
                df['END_POINT'] = df['END_POINT'].apply(lambda x: x.replace("'", ''))
                df['BEGIN_TIME'] = df['BEGIN_TIME'].astype('datetime64')
                df['END_TIME'] = df['END_TIME'].astype('datetime64')
                #df['SCORE'] = df['TRACK_LABEL'].apply(lambda x: self.track_score if '异常' in x else 0) + \
                #            df['TIME_LABEL'].apply(lambda x: self.time_score if '异常' in x else 0) + \
                #            df['WAIT_LABEL'].apply(lambda x: self.wait_score if '发现' in x else 0)
                df['SCORE'] = 0
                df['DOC_NUM'] = self.preent_no
                df = df.reset_index().rename(columns={'index': 'ID'})
            #res.append(df.to_dict('records')[0])
        Write_Oracle_Alarm().write_oracle(_name_BD_RISK_RESULT_TRACK_TR1, df, org_code=None, alarm=None,iscurrent = False)

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
        child_task_id = sys.argv[1]
    else:
        child_task_id = sys.argv[1]

    latest_day = -1    #取最近一天的数据进行处理

    # param_json = '{"track_sim":50,"track_time":40, "wait_time":20, "track_score":-0.8,"time_score":-0.5,"wait_score":-0.8}'
    org_code, param_json, basetime = read_log_table(child_task_id)
    time_start = (date.today()+timedelta(days=latest_day)).strftime('%Y-%m-%d')+" 00:00:00"
    time_end = (date.today()+timedelta(days=latest_day)).strftime('%Y-%m-%d')+" 23:59:59"
    date_filter = " AND PASS_TIME>=to_date('{}','yyyy-mm-dd HH24:mi:ss') AND PASS_TIME<=to_date('{}','yyyy-mm-dd HH24:mi:ss') ".format(time_start,time_end)

    sql_text = '''
        SELECT 
        VEHICLE_NO,
        IN_EXP_TYPE,
        IE_TYPECD,
        BIZOP_ETPS_NO,
        BIZOP_ETPS_NM,
        BIZOP_ETPS_SCCD,
        PASS_TIME,
        AREA,
        PREENT_NO
        FROM DW_CUS_RC.PORT_RELEASE_BSC 
        where IN_EXP_TYPE in ('1','5')
        and PORT_IOCHKPT_STUCD = '2'
        and ISCURRENT = 1
                '''
    sql_text += date_filter 
    car_info_df = Read_Oracle().read_oracle(sql=sql_text, database='dbalarm')
    for i in range(0, len(car_info_df)):
        GpsModel(org_code="", params=param_json, base_time="", child_task_id=child_task_id, info=car_info_df.iloc[i]).run_model_tr1()
