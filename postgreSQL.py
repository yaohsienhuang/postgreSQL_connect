import sys
import time
import psycopg2 as psycopg
import pandas as pd
sys.path.append('../')
from db_conn_info.InitConnInfo import initConnInfo
from db_conn_info.Env import env

class dbConInfo(initConnInfo):
    def __init__(self,env_name, show_info_message = True, logger = None):
        super().__init__(env_name)
        self.__host = env.host()
        self.__port = env.port()
        self.__sid = env.sid()
        self.__acc = env.acc()
        self.__pw = env.pw()
        self.__show_info_message = show_info_message
        self.__logger = logger
        self.__conn_string = "host={0} port={1} dbname={2} user={3} password={4} sslmode={5}".format(self.__host, self.__port, self.__sid, self.__acc, self.__pw, 'allow')
    
    def __enter__(self):
        try:
            self.__db = psycopg.connect(self.__conn_string)
            self.__cursor = self.__db.cursor() 
            self.__start_time = time.time()
        except Exception as e:
            self.__logger.error('connect error!!')
            self.__logger.error(str(e))
        return self
        
    def __exit__(self, type, value, traceback):
        self.__cursor.close()
        self.__db.close()
            
    def only_execute(self, sql):
        result = False
        log_head = '[%s][only_execute]' % (self.__class__.__name__ )
        if self.__show_info_message:
            print(log_head + 'Start')
            print(log_head + 'SQL=', sql)
        try:
            self.__cursor.execute(sql)
            result = True
        except Exception as e:
            print(log_head + '[Error]', e)
            self.__logger.error(str(e))
            self.__logger.error('sql only_execute error')
            self.__logger.error('sql: ' + sql)
        return result
        
    def only_commit(self):
        result = False
        log_head = '[%s][only_commit]' % (self.__class__.__name__ )
        try:
            self.__db.commit()
            result = True
        except Exception as e:
            print(log_head + '[Error]', e)
            self.__logger.error(str(e))
            self.__logger.error('sql only_commit error')
        end_time = time.time()    
        if self.__show_info_message:
            print(log_head + 'End, using time = %6.1f s' % (end_time - self.__start_time))
        return result
        
    def only_executemany(self, sql, values):
        result = False
        log_head = '[%s][only_executemany]' % (self.__class__.__name__ )
        if self.__show_info_message:
            print(log_head + 'Start')
            print(log_head + 'SQL=', sql)
            print(log_head + 'values=', values)
        try:
            self.__cursor.executemany(sql, values)
            result = True
        except Exception as e:
            print(log_head + '[Error]', e)
            self.__logger.error(str(e))
            self.__logger.error('sql only_executemany error')
            self.__logger.error('sql: ' + sql)
        return result
    
    def execute_sqls(self, sql_array):
        start_time = time.time()
        result = False
        log_head = '[%s][execute_sqls]' % (self.__class__.__name__ )
        if self.__show_info_message:
            print(log_head + 'Start')
        try:
            db = psycopg.connect(self.__conn_string)
            try:        
                cursor = db.cursor() 
                for sql in sql_array:
                    if self.__show_info_message:
                        print(log_head + 'SQL=', sql)
                    cursor.execute(sql)
                
                db.commit()
                result = True
            except Exception as e:
                print(log_head + '[Error]', e)
                self.__logger.error(str(e))
                self.__logger.error('sql execute_sqls error')
                self.__logger.error('sql: ' + sql)
            finally:
                cursor.close()
                db.close()
        except Exception as e:
            print(log_head + '[Error]', e)
            self.__logger.error('connect error!!')
            self.__logger.error(str(e))
        end_time = time.time()
        if self.__show_info_message:
            print(log_head + 'End, using time = %6.1f s' % (end_time - start_time))
        return result
        
    def select(self, sql):
        start_time = time.time()
        result = None
        log_head = '[%s][select]' % self.__class__.__name__
        if self.__show_info_message:
            print(log_head + 'Start')
        try:
            if self.__show_info_message:
                print(self.__conn_string)
            db = psycopg.connect(self.__conn_string)
            if self.__show_info_message:
                print(log_head + 'SQL=', sql)
            try:
                result = pd.read_sql(sql, db)
                if self.__show_info_message:
                    print(log_head + 'result_count =', result.shape[0])
            except Exception as e:
                print(log_head + '[Error]', e)
                self.__logger.error(str(e))
                self.__logger.error('sql error')
                self.__logger.error('sql: ' + sql)
            finally:
                db.close
        except Exception as e:
            print(log_head + '[Error]', e)
            self.__logger.error('connect error!!')
            self.__logger.error(str(e))

        end_time = time.time()
        if self.__show_info_message:
            print(log_head + 'End, using time = %6.1f s' % (end_time - start_time))
        return result
    
    def execute(self, sql):
        start_time = time.time()
        result = False
        log_head = '[%s][execute]' % (self.__class__.__name__ )
        if self.__show_info_message:
            print(log_head + 'Start')
        try:
            db = psycopg.connect(self.__conn_string)
            if self.__show_info_message:
                print(log_head + 'SQL=', sql)
            try:        
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
                result = True
            except Exception as e:
                print(log_head + '[Error]', e)
                self.__logger.error(str(e))
                self.__logger.error('sql execute error')
                self.__logger.error('sql: ' + sql)
            finally:
                cursor.close()
                db.close()
        except Exception as e:
            print(log_head + '[Error]', e)
            self.__logger.error('connect error!!')
            self.__logger.error(str(e))
        end_time = time.time()
        if self.__show_info_message:
            print(log_head+'End, using time = %6.1f s' % (end_time - start_time))
        return result
    
    def executemany(self, sql, values):
        start_time = time.time()
        result = False
        log_head = '[%s][executemany]' % (self.__class__.__name__ )
        if self.__show_info_message:
            print(log_head + 'Start')
        try:
            db = psycopg.connect(self.__conn_string)
            cursor = db.cursor() #建立遊標
            if self.__show_info_message:
                print(log_head + 'SQL=', sql)
            try:        
                cursor.executemany(sql,values)
                db.commit()
                result = True
            except Exception as e:
                print(log_head + '[Error]', e)
                self.__logger.error(str(e))
                self.__logger.error('sql executemany error')
                self.__logger.error('sql: ' + sql)
            finally:
                cursor.close()
                db.close()
        except Exception as e:
            print(log_head + '[Error]', e)
            self.__logger.error('connect error!!')
            self.__logger.error(str(e))
        end_time = time.time()
        if self.__show_info_message:
            print(log_head + 'End, using time = %6.1f s' % (end_time - start_time))
        return result

    
def dbConInfo_deco(env_name):
    def decorator(func):
        def with_connect(*args, **kwargs):
            with dbConInfo(env_name) as dci:
                result = func(dci, *args, **kwargs)
            return result
        return with_connect
    return decorator
    
    
if __name__ == '__main__':
    
    @dbConInfo_deco('line1_info')
    def select_db(dci):
        dci_result = None
        sql = f'''
            Select * From public.record
            WHERE lot_id='cccc'
            AND time_folder='eeee' 
            AND path_serial='yyyy'
        '''
        dci_result = dci.select(sql)
        return dci_result
    
    result=select_db()
    print(result)
