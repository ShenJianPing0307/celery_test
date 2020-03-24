# https://www.jianshu.com/p/3bcb98dd2654
# 数据库配置
class sqlConfig:
    db_name = "test_db"           # 数据库名
    db_user = "root"              # 用户名
    db_host = "localhost"         # IP地址
    db_port = 3306                # 端口号
    db_passwd = "1202"            # 密码
# 数据库连
# 2种连接方式
#
# 通过MySQLdb连接
    def sql_con(self):
        # 打开数据库连接，此方法指定数据库
        conn = MySQLdb.connect(
            host=sqlConfig.db_host,
            user=sqlConfig.db_user,
            passwd=sqlConfig.db_passwd,
            db=sqlConfig.db_name,
            charset="utf8"
        )
        return conn
    def sql_con_without_database_name(self):
        # 打开数据库连接，不指定数据库。需要重新创建数据库的时候用这个
        conn = MySQLdb.connect(
            host=sqlConfig.db_host,
            user=sqlConfig.db_user,
            passwd=sqlConfig.db_passwd,
            charset="utf8"
        )
        return conn
# 通过sqlalchemy模块
from sqlalchemy import create_engine
 # 初始化数据库连接，使用pymysql模块
        db_info = {
            'user': sqlConfig.db_user,
            'password': sqlConfig.db_passwd,
            'host': sqlConfig.db_host,
            'port': sqlConfig.db_port,
            'database': sqlConfig.db_name
        }
        engine = create_engine(
            'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8' % db_info,
            encoding='utf-8'
        )
# 创建数据库
    def create_database(self, database_name):
        # 先创建连接
        conn = self.sql_con_without_database_name()
        cursor = conn.cursor()
        create_database_str = 'CREATE DATABASE %s;' % database_name
        cursor.execute(create_database_str)
        # 断开连接
        cursor.close()
        conn.close()
# 创建表
    def create_table(self, create_table_str):
        # # mysql 创建表语言，表结构实例
        # create_table_str = '''create table student(
        #     id int,
        #     name char(16),
        #     born_year year,
        #     birth date,
        #     class_time time,
        #     reg_time datetime
        # );
        # '''
        # 先创建连接
        conn = self.sql_con()
        cursor = conn.cursor()
        cursor.execute(create_table_str)
        # 断开连接
        cursor.close()
        conn.close()
# 插入数据
# 此方法通过调用dataframe.to_sql函数实现，但是该功能是以表为单位进行数据追加或者替换整张表。不能实现替换数据中已存在部分、插入未存在的数据的功能。
    def insert_data_by_pandas(self, dataframe, table_name, if_exists='append'):
        '''
        通过dataframe 向 sql 中插入表，此方法缺点是若表已存在，不能替换表中部分重复数据，只能替换/追加整张表
        :param dataframe: pd.Dataframe类型
        :param table_name: 插入的表名
        :param if_exists: {'fail', 'replace', 'append'}, default 'fail'
            - fail: If table exists, do nothing.
            - replace: If table exists, drop it, recreate it, and insert data.
            - append: If table exists, insert data. Create if does not exist.
        :return:
        '''
        # 初始化数据库连接，使用pymysql模块
        db_info = {
            'user': sqlConfig.db_user,
            'password': sqlConfig.db_passwd,
            'host': sqlConfig.db_host,
            'port': sqlConfig.db_port,
            'database': sqlConfig.db_name
        }
        engine = create_engine(
            'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8' % db_info,
            encoding='utf-8'
        )
        dataframe.to_sql(table_name, engine, if_exists=if_exists, index=False, chunksize=100)
# 此方法通过sql插入语句进行批量插入操作。
    def insert_data_multi(self, dataframe, table_name):
        '''
        通过dataframe 向 sql 中批量插入数据
        :param dataframe: pd.Dataframe类型
        :param table_name: 插入的表名
        :return:
        '''
        # 先创建连接
        conn = self.sql_con()
        cursor = conn.cursor()
        # 获取列名和值
        keys = dataframe.keys()
        values = dataframe.values.tolist()
        key_sql = ','.join(keys)
        value_sql = ','.join(['%s'] * dataframe.shape[1])
        # 插入语句
        insert_data_str = """ insert into %s (%s) values (%s)""" % (table_name, key_sql, value_sql)
        # 提交数据库操作
        cursor.executemany(insert_data_str, values)
        conn.commit()
        # 断开连接
        cursor.close()
        conn.close()
#对数据进行更新
#此方法在插入数据时，若数据已存在，则替换其为新数据，否则追加到数据表中。数据表中必须存在主键或者某一字段属性为“UNIQUE”。

    def update_data_multi(self, dataframe, table_name):
        '''
        通过dataframe 向 sql 中批量插入数据
        :param dataframe: pd.Dataframe类型
        :param table_name: 插入的表名
        :return:
        '''
        # 先创建连接
        conn = self.sql_con()
        cursor = conn.cursor()
        # 获取列名和值
        keys = dataframe.keys()
        values = dataframe.values.tolist()
        key_sql = ','.join(keys)
        value_sql = ','.join(['%s'] * dataframe.shape[1])
        # 插入语句，若数据已存在则更新数据
        insert_data_str = """ insert into %s (%s) values (%s) ON DUPLICATE KEY UPDATE""" % (table_name, key_sql, value_sql)
        update_str = ','.join([" {key} = VALUES({key})".format(key=key) for key in keys])
        insert_data_str += update_str
        # 提交数据库操作
        cursor.executemany(insert_data_str, values)
        conn.commit()
        # 断开连接
        cursor.close()
        conn.close()