import sys, os
from os import path
sys.path.append('/root/bdrisk/risk_project')
sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
from risk_models import *
from risk_models import _name_BD_RISK_WAREHOUSE_ORDER_HEAD, _name_BD_RISK_WAREHOUSE_ORDER, _name_WAREHOUSE_STOCK_BILL


class WarehouseCleanWh2(object):
    def __init__(self, child_task_id, org_code, params):
        self.child_task_id = child_task_id
        Risk_logger(child_task_id = self.child_task_id, exec_status=None)

        # 参数读取
        self.org_code = org_code
        self.type = json.loads(params)['type']
        
    def clean_wh2(self):
        if self.type == 'hard':
            # 获取已完成仓储订单列表
            WAREHOUSE_ORDER_LIST = Read_Oracle().read_oracle(sql= 
            """ 
            select b.order_no, complete_date from 
            (select order_no, sum(to_number(QTY_ORDER)) as QTY_ORDER
            from {}
            where CAPXACTION != 'D' 
            and org_code = '{}'
            and order_type != '3'
            group by order_no) a
            inner join
            (select order_no, sum(to_number(QTY)) as QTY_WH, max(actrual_stock_date) as complete_date
            from {} 
            where iscurrent =1 
            and ORG_CODE  = '{}'
            group by order_no) b
            on a.order_no = b.order_no 
            where a.QTY_ORDER = b.QTY_WH
            
            union all
            
            select b.order_no, complete_date from 
            (select order_no, sum(to_number(QTY_ORDER)) as QTY_ORDER
            from {}
            where CAPXACTION != 'D' 
            and org_code = '{}'
            and order_type = '3'
            group by order_no) a
            inner join
            (select order_no, sum(to_number(QTY)) as QTY_WH, max(actrual_stock_date) as complete_date
            from {} 
            where iscurrent =1 
            and ORG_CODE  = '{}'
            and STOCK_BILL_TYPE = '2'
            group by order_no) b
            on a.order_no = b.order_no 
            where a.QTY_ORDER = b.QTY_WH
            """.format(_name_BD_RISK_WAREHOUSE_ORDER, self.org_code, _name_WAREHOUSE_STOCK_BILL, self.org_code, 
                       _name_BD_RISK_WAREHOUSE_ORDER, self.org_code, _name_WAREHOUSE_STOCK_BILL, self.org_code), database = 'dbods')
            
            # 获取风控企业的所有订单初始时间
            WAREHOUSE_ORDER_LIST_START = Read_Oracle().read_oracle(sql= 
            """ 
            select customer_name, order_type, order_no, min(order_date) as order_date
            from {}
            where CAPXACTION != 'D' 
            and org_code = '{}'
            and capxaction != 'D'
            group by customer_name, order_type, order_no
            """.format(_name_BD_RISK_WAREHOUSE_ORDER, self.org_code), database = 'dbods')
            
            # 汇总订单起始和结束时间
            WAREHOUSE_ORDER_DETAIL = pd.merge(WAREHOUSE_ORDER_LIST, WAREHOUSE_ORDER_LIST_START, how='left', on = ['ORDER_NO'])
            
            # 限制 COMPLETE_DATE 必须晚于 ORDER_DATE
            WAREHOUSE_ORDER_DETAIL = WAREHOUSE_ORDER_DETAIL[WAREHOUSE_ORDER_DETAIL['COMPLETE_DATE'] >= WAREHOUSE_ORDER_DETAIL['ORDER_DATE']]
            # 计算完成订单时间差
            WAREHOUSE_ORDER_DETAIL['DIFF_DAYS'] = (WAREHOUSE_ORDER_DETAIL['COMPLETE_DATE'] - WAREHOUSE_ORDER_DETAIL['ORDER_DATE']).map(lambda x: x/np.timedelta64(1,'D'))
            
            # 加入企业信息; ID; 模型运行时间
            WAREHOUSE_ORDER_DETAIL['ORG_CODE'] = self.org_code
            WAREHOUSE_ORDER_DETAIL['ID'] = range(len(WAREHOUSE_ORDER_DETAIL))
            detail_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            WAREHOUSE_ORDER_DETAIL['CHECK_TIME'] = datetime.datetime.strptime(detail_now, "%Y-%m-%d %H:%M:%S")
            
            # 明确企业所属租户
            WAREHOUSE_ORDER_DETAIL['CUSTOMER_CODE'] = 'FTA_LG'
            
            # 整理明细表
            WAREHOUSE_ORDER_DETAIL = WAREHOUSE_ORDER_DETAIL[['ID', 'ORG_CODE', 'CUSTOMER_NAME', 'ORDER_TYPE', 'ORDER_NO', 'ORDER_DATE', 'COMPLETE_DATE', 'DIFF_DAYS', 'CHECK_TIME','CUSTOMER_CODE']]
            
            # 写入数据库
            Write_Oracle().write_oracle('BD_RISK_DETAIL_WAREHOUSE_WH2',WAREHOUSE_ORDER_DETAIL, org_code = self.org_code, alarm = None)
            
        elif self.type == 'easy':
            # 读取订单头表
            WAREHOUSE_ORDER_DETAIL = Read_Oracle().read_oracle(sql= """ select * from {} where CAPXACTION != 'D' and org_code = '{}' 
                                                                    and status = '1' """.format(_name_BD_RISK_WAREHOUSE_ORDER_HEAD, self.org_code), database = 'dbods')
            
            # 限制 COMPLETE_DATE 必须晚于 ORDER_DATE
            WAREHOUSE_ORDER_DETAIL = WAREHOUSE_ORDER_DETAIL[WAREHOUSE_ORDER_DETAIL['COMPLETE_DATE'] >= WAREHOUSE_ORDER_DETAIL['ORDER_DATE']]
            # 计算完成订单时间差
            WAREHOUSE_ORDER_DETAIL['DIFF_DAYS'] = (WAREHOUSE_ORDER_DETAIL['COMPLETE_DATE'] - WAREHOUSE_ORDER_DETAIL['ORDER_DATE']).map(lambda x: x/np.timedelta64(1,'D'))
            
            # 加入ID; 模型运行时间
            WAREHOUSE_ORDER_DETAIL['ID'] = range(len(WAREHOUSE_ORDER_DETAIL))
            detail_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            WAREHOUSE_ORDER_DETAIL['CHECK_TIME'] = datetime.datetime.strptime(detail_now, "%Y-%m-%d %H:%M:%S")
            
            # 明确企业所属租户
            WAREHOUSE_ORDER_DETAIL['CUSTOMER_CODE'] = 'FTA_LG'
            
            # 整理明细表
            WAREHOUSE_ORDER_DETAIL = WAREHOUSE_ORDER_DETAIL[['ID', 'ORG_CODE', 'CUSTOMER_NAME', 'ORDER_TYPE', 'ORDER_NO', 'ORDER_DATE', 'COMPLETE_DATE', 'DIFF_DAYS', 'CHECK_TIME','CUSTOMER_CODE']]
            
            # 写入数据库
            Write_Oracle().write_oracle('BD_RISK_DETAIL_WAREHOUSE_WH2',WAREHOUSE_ORDER_DETAIL, org_code = self.org_code, alarm = None)
            
        else:
            print('输入类型错误，请重试！')

    def run_clean_wh2(self):
        try:
            self.clean_wh2()
            exec_status = 1
        except:
            logger.exception('model execution error')
            exec_status = 0
        finally:
            Risk_logger(child_task_id = self.child_task_id, exec_status=exec_status).write_log()


if __name__ == '__main__':
    # 读取传入的 child_task_id，仅适用于py调用,先备注掉
    child_task_id = sys.argv[1]
    # child_task_id = '0001_0021'
    org_code, params, base_time = read_log_table(child_task_id)
    WarehouseCleanWh2(child_task_id, org_code, params).run_clean_wh2()
