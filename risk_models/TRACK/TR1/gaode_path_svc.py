import sys, os
from os import path
from datetime import date,timedelta
import cx_Oracle
import json
'''
if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk_model\risk_model")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
    
'''
sys.path.append("../../../")
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))

from risk_models.config.auth_config import *



account = ezpass_db.Authconfig.oracle_write_account_alarm
uri = ezpass_db.Authconfig.oracle_write_address_alarm 



DB_URI="192.168.129.149:1521/test12c"
DB_USER="src_EXTCOLLECT"
DB_PASSWORD="easipass"
LATEST_DAYS = -3

def get_conn():

   
    
    DB_USER = account.split(":")[0]
    DB_PASSWORD = account.split(":")[1]
    DB_URI = uri.replace("?service_name=","")
    print("oracle uri: ",DB_URI)
    print("user:",DB_USER)
    conn = cx_Oracle.connect(user=DB_USER,password=DB_PASSWORD,dsn=DB_URI)
    
    return conn


"""
    获取高德地图路径两点之间预测路径坐标最近一天的所有列表
    返回数据为一个dict, 其元素0_path,0_time, 0_path值为列表格式如下：
    [
    [[121.566674, 31.367871], [121.566106, 31.366916],],
    [[121.614886, 31.336356], [121.615896, 31.335561],],
    [[121.765181, 31.148301], [121.765529, 31.147351],],
    ]
    ，0_time 为路径消耗时间(秒)
    如果程序出错，返回零元素列表[]

"""
def get_gps_coordinates(begin_point_name:str, end_point_name:str):
    
    try:
    
        conn = get_conn()
        
        cursor = conn.cursor()
        last_date = (date.today()+timedelta(days=LATEST_DAYS)).strftime('%Y-%m-%d')
        
        
        # BD_RISK_LOGISTIC_GAODE_GPS | GAODE_P2P_PATH
        sql = "select route, to_char(LASTUPDATE,'yyyy-mm-dd'), DURATION  from \
                BD_RISK_LOGISTIC_GAODE_GPS where to_char(LASTUPDATE,'yyyy-mm-dd')>='{}' \
                and BEGIN_POINT_NAME='{}' and END_POINT_NAME='{}' order by LASTUPDATE desc".format(last_date,begin_point_name,end_point_name)
                
        
        
        print("sql:",sql)
        
        cursor.execute(sql)
        
        pre_day = ""
        route_ls = []
        route_info = {}
        for row in cursor.fetchall():
            route_info["0_path"] = json.loads(row[0].read())
            route_info["0_time"] = row[2]
            if pre_day=="":
                route_ls.append(route_info)
                pre_day = row[1]
            elif pre_day== row[1]:
                route_ls.append(route_info)
            else:
                break
            
        return route_ls

            
            
    except Exception as inst:
        print("oracle execute error")
        print(inst)    
    
    return []


if __name__=='__main__':
    rls = get_gps_coordinates('外二码头卡口A','洋山特殊综合保税区5号卡口（保税）')
    
    print("find route number:",len(rls))
    print(rls[-1])