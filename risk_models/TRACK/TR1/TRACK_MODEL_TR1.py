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


class GpsModel1(object):
    def __init__(self, org_code, params, base_time, child_task_id):
        self.model_code = 'TRACK'
        self.child_model_code = 'TR1'
        self.child_task_id = child_task_id
        Risk_logger(child_task_id=self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.track_tar = json.loads(params)['track_sim']
        self.time_tar = json.loads(params)['track_time']
        self.wait_time_tar = json.loads(params)['wait_time']
        self.track_score = json.loads(params)['track_score']
        self.time_score = json.loads(params)['time_score']
        self.wait_score = json.loads(params)['wait_score']

    def get_start(self):
        pass

    def get_end(self):
        pass

    def parse_target(self, target):
        id = target['ID']
        org_code = target['UNF_SC_ID']
        doc_type = target['DOCU_TYPE']
        doc_num = target['DOCU_CODE']
        carnum = target['CTNR_LORRY_LPN']
        start_time = target['VEHICLE_DEPT_DATE']
        end_time = target['VEHICLE_ARRIVAL_DATE']
        start_point = target['VEHICLE_DEPT_PLACE_LON'] + ',' + target['VEHICLE_DEPT_PLACE_LAT']
        end_point = target['VEHICLE_ARRIVAL_PLACE_LON'] + ',' + target['VEHICLE_ARRIVAL_PLACE_LAT']
        return id, org_code, doc_type, doc_num, carnum, start_time, end_time, start_point, end_point

    def read_track_info(self):
        sql = f'''select * from {_name_BD_RISK_TRACK_INFO} where validated_flag = 2'''
        df = Read_Oracle().read_oracle(sql=sql, database='dbdm')
        target_dict = df.to_dict('records')
        return target_dict

    def model_tr1(self):
        target_dict = self.read_track_info()
        res=[]
        for target in target_dict:
            id, org_code, doc_type, doc_num, carnum, start_time, end_time, start_point, end_point = self.parse_target(
                target)
            real_track = get_real_track(carnum, start_time, end_time, version='new')
            pred_track = get_pred_track(begin_point=start_point, end_point=end_point)
            tracksim, realtime, realpath, predtime, predpath, timerisk, waitlocation = \
                TrackDetection.track_detection(real_track, pred_track)
            df = {}
            df['ORG_CODE'] = org_code
            df['DOC_NUM'] = doc_num
            df['DOC_TYPE'] = doc_type
            df['VEHICLE_NO'] = carnum
            # TODO 需要切换 0 和 1 的位置
            # 注意这里的begin——point 和 end——point都是反的 31，121
            df['BEGIN_POINT'] = [float(start_point.split(',')[0]), float(start_point.split(',')[1])]
            df['BEGIN_TIME'] = start_time
            df['END_POINT'] = [float(end_point.split(',')[0]), float(end_point.split(',')[1])]
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
            df['CUSTOMER_CODE'] = 'FTA_LG'
            Write_Oracle().write_oracle(_name_BD_RISK_RESULT_TRACK_TR1, df, org_code=None, alarm=None)
            Write_Track().update_track_info(id=id)
        df_res = pd.DataFrame(res)
        track_num = df_res['TRACK_LABEL'].apply(lambda x: 1 if '正常' not in x else 0).sum()
        time_num = df_res['TIME_LABEL'].apply(lambda x: 1 if '正常' not in x else 0).sum()
        wait_num = df_res['WAIT_LABEL'].apply(lambda x: 1 if '无' not in x else 0).sum()
        alarm_reason = ['发现{}起路径异常事件'.format(track_num),
                        '发现{}起货车运行时间异常事件'.format(time_num),
                        '发现{}起货车异常停靠事件'.format(wait_num),
                        ]
        df_alarm = []
        for i in alarm_reason:
            temp = {}
            temp['CHECK_TIME'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            temp['ORG_CODE'] = self.org_code
            temp['MODEL_CODE'] = self.model_code
            temp['CHILD_MODEL_CODE'] = self.child_model_code
            temp['ALARM_REASON'] = i
            temp['ALARM_NUMBER'] = int(re.findall('发现(.*?)起.*', i)[0])
            df_alarm.append(temp)
        df_alarm = pd.DataFrame(df_alarm)
        df_alarm = df_alarm[df_alarm['ALARM_NUMBER'] != 0]
        df_alarm = df_alarm.reset_index().rename(columns={'index': 'ID'})
        df_alarm['CHECK_TIME'] = pd.to_datetime(df_alarm['CHECK_TIME'], format='%Y-%m-%d %H:%M:%S')
        df_alarm['CUSTOMER_CODE'] = 'FTA_LG'
        print(df_alarm)
        if df_alarm.empty:
            print('没有异常情况')
        else:
            Write_Oracle().write_oracle('BD_RISK_ALARM_ITEM', df_alarm, org_code=self.org_code,
                                        alarm=[self.model_code, self.child_model_code])

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
    org_code, param_json, base_time = read_log_table(child_task_id)
    GpsModel1(org_code=org_code, params=param_json, base_time=base_time, child_task_id=child_task_id).run_model_tr1()
