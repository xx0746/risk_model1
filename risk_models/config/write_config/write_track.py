import os
from loguru import logger
from sqlalchemy import create_engine
from risk_models.config.auth_config import ezpass_db
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

BD_RISK_TRACK_INFO = 'BD_RISK_TRACK_INFO'
class Write_Track(object):
    def __init__(self):
        self.write_account = ezpass_db.Authconfig.oracle_write_account.value
        self.write_address = ezpass_db.Authconfig.oracle_write_address.value
        # 目前只考虑一个conn，以后有多个写入地址再加
        self.conn = create_engine(f"oracle+cx_oracle://{self.write_account}@{self.write_address}",
                                  encoding='utf-8', convert_unicode=True)
        self.conn.autocommit = True

    @logger.catch()
    def update_track_info(self, id):
        sql = f'''update {BD_RISK_TRACK_INFO} set validated_flag = 1 where ID ={id}'''
        self.conn.execute(sql)
        logger.info(f'Updated id {id } in {BD_RISK_TRACK_INFO} successfully!')