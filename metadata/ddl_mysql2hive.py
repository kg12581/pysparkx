import pymysql
from pyhive import hive
from typing import List, Dict
import re

from pyspark.sql.functions import upper
from soupsieve.util import lower


class MySQL2Hive:
    def __init__(
            self,
            # MySQL 配置
            mysql_host: str,
            mysql_port: int,
            mysql_user: str,
            mysql_password: str,
            mysql_db: str,
            # Hive 配置
            hive_host: str,
            hive_port: int = 10000,
            hive_user: str = "hive",
            hive_database: str = "default",
            # Hive 表存储配置
            hive_location: str = "/user/hive/warehouse/{db}.db/{table}",
            file_format: str = "ORC"  # 支持 ORC, PARQUET, TEXTFILE 等
    ):
        # 初始化MySQL连接
        self.mysql_conn = self._connect_mysql(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            db=mysql_db
        )

        # 初始化Hive连接
        self.hive_cursor = self._connect_hive(
            host=hive_host,
            port=hive_port,
            user=hive_user,
            database=hive_database
        )

        self.hive_database = hive_database
        self.hive_location = hive_location
        self.file_format = file_format.upper()

        # MySQL到Hive的数据类型映射表
        self.type_mapping = {
            # 整数类型
            'int': 'INT',
            'tinyint': 'TINYINT',
            'smallint': 'SMALLINT',
            'mediumint': 'INT',
            'bigint': 'BIGINT',
            # 浮点类型
            'float': 'FLOAT',
            'double': 'DOUBLE',
            'decimal': 'DECIMAL',
            # 字符串类型
            'char': 'STRING',
            'varchar': 'STRING',
            'text': 'STRING',
            'mediumtext': 'STRING',
            'longtext': 'STRING',
            # 日期时间类型
            'date': 'DATE',
            'time': 'STRING',  # Hive的TIME类型使用较少，通常用STRING
            'datetime': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            # 其他类型
            'boolean': 'BOOLEAN',
            'json': 'STRING',  # Hive对JSON的支持通过函数实现，存储为STRING
            'blob': 'BINARY'
        }

    def _connect_mysql(self, host: str, port: int, user: str, password: str, db: str) -> pymysql.connections.Connection:
        """连接MySQL数据库"""
        try:
            conn = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                db=db,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            print("✅ 成功连接到MySQL数据库")
            return conn
        except Exception as e:
            raise RuntimeError(f"❌ MySQL连接失败: {str(e)}") from e

    def _connect_hive(self, host: str, port: int, user: str, database: str) -> hive.Cursor:
        """连接Hive数据库"""
        try:
            conn = hive.Connection(
                host=host,
                port=port,
                username=user,
                database=database,
                auth='NONE'  # 根据实际认证方式修改，如'LDAP'
            )
            cursor = conn.cursor()
            print("✅ 成功连接到Hive数据库")
            return cursor
        except Exception as e:
            raise RuntimeError(f"❌ Hive连接失败: {str(e)}") from e

    def get_mysql_tables(self) -> List[str]:
        """获取MySQL当前数据库中所有用户表"""
        try:
            with self.mysql_conn.cursor() as cursor:
                cursor.execute("""
                               SELECT table_name
                               FROM information_schema.tables
                               WHERE table_schema = %s
                                 AND table_type = 'BASE TABLE'
                               ORDER BY table_name
                               """,(self.mysql_conn.db,))
                tables = [row['table_name'.upper()] for row in cursor.fetchall()]
                print(f"✅ 发现 {len(tables)} 个MySQL表需要同步")
                return tables
        except Exception as e:
            raise RuntimeError(f"❌ 获取MySQL表列表失败: {str(e)}") from e

    def get_mysql_table_schema(self, table_name: str) -> List[Dict]:
        """获取MySQL表的字段结构"""
        try:
            with self.mysql_conn.cursor() as cursor:
                cursor.execute("""
                               SELECT column_name,
                                      data_type,
                                      IF(is_nullable = 'YES', 1, 0) as is_nullable,
                                      column_comment,
                                      column_default
                               FROM information_schema.columns
                               WHERE table_schema = %s
                                 AND table_name = %s
                               ORDER BY ordinal_position
                               """, (self.mysql_conn.db, table_name))
                schema = cursor.fetchall()
                print(f"✅ 获取表 {table_name} 结构成功，共 {len(schema)} 个字段")
                return schema
        except Exception as e:
            raise RuntimeError(f"❌ 获取表 {table_name} 结构失败: {str(e)}") from e

    def map_mysql_type_to_hive(self, mysql_type: str) -> str:
        """将MySQL数据类型映射为Hive类型"""
        # 提取基础类型（去除括号里的参数）
        base_type = mysql_type.split('(')[0].lower()
        if base_type in self.type_mapping:
            # 特殊处理DECIMAL类型，保留精度信息
            if base_type == 'decimal':
                match = re.search(r'decimal\((\d+),(\d+)\)', mysql_type.lower())
                if match:
                    return f"DECIMAL({match.group(1)},{match.group(2)})"
            return self.type_mapping[base_type]
        # 处理未映射的类型，默认使用STRING
        print(f"⚠️ 未找到 {mysql_type} 的映射关系，使用STRING替代")
        return "STRING"

    def generate_hive_create_sql(self, table_name: str, table_schema: List[Dict]) -> str:
        """生成Hive表的创建SQL"""
        if not table_schema:
            raise ValueError(f"表 {table_name} 没有字段信息，无法生成建表语句")

        # 构建字段定义
        columns = []
        for field in table_schema:
            field_name = field['column_name'.upper()]
            mysql_type = field['data_type'.upper()]
            hive_type = self.map_mysql_type_to_hive(mysql_type)

            # 处理注释（转义单引号）
            comment = field['column_comment'.upper()].replace("'", "\\'")
            comment_clause = f" COMMENT '{comment}'" if comment else ""

            # Hive通常不强制非空约束，这里仅做注释说明
            # nullable_comment = " -- 允许为NULL" if field['is_nullable'] else " -- 不允许为NULL"

            # 拼接字段定义
            # columns.append(f"`{field_name}` {hive_type}{comment_clause}{nullable_comment}")
            columns.append(f"`{field_name}` {hive_type}{comment_clause}")

        # 计算存储路径
        location = self.hive_location.format(db=self.hive_database, table=table_name)

        # 生成建表SQL（Hive特有语法）
        create_sql = f"""
CREATE TABLE IF NOT EXISTS `{self.hive_database}`.`{table_name}` (
    {(''',    
      ''').join(columns)}
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS {self.file_format}
        """.strip()

        return create_sql

    def create_hive_table(self, create_sql: str) -> bool:
        """执行SQL创建Hive表"""
        try:
            self.hive_cursor.execute(create_sql)
            print(f"✅ 执行建表SQL成功:\n{create_sql[:200]}...")  # 只显示前200字符
            return True
        except Exception as e:
            print(f"❌ 执行建表SQL失败: {str(e)}\nSQL: {create_sql}")
            return False

    def batch_sync_tables(self, tables: List[str] = None) -> None:
        """批量同步表结构"""
        # 如果未指定表，则同步所有表
        if not tables:
            tables = self.get_mysql_tables()

        success_count = 0
        fail_count = 0

        for table in tables:
            print(f"\n===== 开始处理表: {table} =====")
            try:
                # 获取表结构
                schema = self.get_mysql_table_schema(table)
                if not schema:
                    print(f"⚠️ 表 {table} 没有字段，跳过")
                    continue

                # 生成建表SQL
                create_sql = self.generate_hive_create_sql(table, schema)

                # 执行创建表
                if self.create_hive_table(create_sql):
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                print(f"❌ 处理表 {table} 失败: {str(e)}")

        print(f"\n===== 同步完成 =====")
        print(f"总表数: {len(tables)}, 成功: {success_count}, 失败: {fail_count}")

    def close_connections(self) -> None:
        """关闭数据库连接"""
        if self.mysql_conn:
            self.mysql_conn.close()
            print("✅ 已关闭MySQL连接")
        if self.hive_cursor:
            self.hive_cursor.close()
            # self.hive_cursor.connection.close()
            print("✅ 已关闭Hive连接")


if __name__ == "__main__":
    # 配置信息（请根据实际环境修改）
    config = {
        # MySQL配置
        "mysql": {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "Admin@123456",
            "db": "qianfeng"
        },
        # Hive配置（默认端口10000）
        "hive": {
            "host": "localhost",
            "port": 10000,
            "user": "kgt",
            "database": "qianfeng",
            # "location": "/user/hive/warehouse/{db}.db/{table}",  # Hive表存储路径
            "file_format": "TEXTFILE"  # 存储格式：ORC/PARQUET/TEXTFILE
        }
    }

    # 创建同步工具实例
    sync_tool = MySQL2Hive(
        mysql_host=config["mysql"]["host"],
        mysql_port=config["mysql"]["port"],
        mysql_user=config["mysql"]["user"],
        mysql_password=config["mysql"]["password"],
        mysql_db=config["mysql"]["db"],
        hive_host=config["hive"]["host"],
        hive_port=config["hive"]["port"],
        hive_user=config["hive"]["user"],
        hive_database=config["hive"]["database"],
        # hive_location=config["hive"]["location"],
        file_format=config["hive"]["file_format"]
    )

    try:
        # 批量同步所有表
        # 如需指定表同步，可以传入参数，例如：["table1", "table2"]
        sync_tool.batch_sync_tables()
    finally:
        # 关闭连接
        sync_tool.close_connections()
