import pymysql
import json
import os
from typing import List, Dict, Optional


class MySQLToPostgreSQLDataX:
    def __init__(self, mysql_host: str, mysql_port: int, mysql_user: str,
                 mysql_password: str, mysql_database: str):
        """初始化MySQL数据库连接信息"""
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_database = mysql_database
        self.connection = None

        # MySQL到PostgreSQL的数据类型映射
        self.type_mapping = {
            # 数值类型
            'int': 'INT4',
            'tinyint': 'INT2',
            'smallint': 'INT2',
            'mediumint': 'INT4',
            'bigint': 'INT8',
            'float': 'FLOAT4',
            'double': 'FLOAT8',
            'decimal': 'NUMERIC',
            'numeric': 'NUMERIC',

            # 字符串类型
            'char': 'VARCHAR',
            'varchar': 'VARCHAR',
            'text': 'TEXT',
            'tinytext': 'TEXT',
            'mediumtext': 'TEXT',
            'longtext': 'TEXT',

            # 日期时间类型
            'date': 'DATE',
            'datetime': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            'time': 'TIME',
            'year': 'INT2',

            # 二进制类型
            'binary': 'BYTEA',
            'varbinary': 'BYTEA',
            'blob': 'BYTEA',
            'tinyblob': 'BYTEA',
            'mediumblob': 'BYTEA',
            'longblob': 'BYTEA',

            # 其他类型
            'enum': 'VARCHAR',
            'set': 'VARCHAR',
            'bit': 'BIT'
        }

    def connect(self) -> bool:
        """连接到MySQL数据库"""
        try:
            self.connection = pymysql.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_user,
                password=self.mysql_password,
                database=self.mysql_database,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            return True
        except pymysql.MySQLError as e:
            print(f"MySQL数据库连接错误: {e}")
            return False

    def get_tables(self) -> List[str]:
        """获取数据库中所有表名"""
        if not self.connection or not self.connection.open:
            print("未连接到数据库")
            return []

        tables = []
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                # 结果是字典列表，键为'Tables_in_数据库名'
                table_key = f'Tables_in_{self.mysql_database}'
                for row in cursor.fetchall():
                    tables.append(row[table_key])
        except pymysql.MySQLError as e:
            print(f"获取表名错误: {e}")

        return tables

    def get_table_columns(self, table_name: str) -> List[Dict]:
        """获取指定表的列信息"""
        if not self.connection or not self.connection.open:
            print("未连接到数据库")
            return []

        columns = []
        try:
            with self.connection.cursor() as cursor:
                # 查询列信息，包括列名和数据类型
                cursor.execute(f"DESCRIBE {table_name}")
                for row in cursor.fetchall():
                    # 提取基础类型（去掉括号中的长度信息）
                    base_type = row['Type'].split('(')[0].lower()
                    # 映射到PostgreSQL类型
                    pg_type = self.type_mapping.get(base_type, 'VARCHAR')

                    columns.append({
                        "name": row['Field'],
                        "mysql_type": row['Type'],
                        "pg_type": pg_type
                    })
        except pymysql.MySQLError as e:
            print(f"获取表{table_name}的列信息错误: {e}")

        return columns

    def generate_datax_config(
            self,
            table_name: str,
            columns: List[Dict],
            mysql_params: Dict,
            pg_params: Dict,
            output_dir: str = "datax_configs"
    ) -> Optional[str]:
        """
        生成DataX配置文件

        修复了column参数格式问题：PostgreSQL Writer需要字符串列表而非对象列表
        """
        # 创建输出目录
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 提取列名列表（修复的核心部分）
        column_names = [col["name"] for col in columns]

        # DataX配置模板
        datax_config = {
            "job": {
                "content": [
                    {
                        "reader": {
                            "name": "mysqlreader",
                            "parameter": {
                                "username": mysql_params.get("username", self.mysql_user),
                                "password": mysql_params.get("password", self.mysql_password),
                                "column": column_names,  # 列名列表
                                "connection": [
                                    {
                                        "table": [table_name],
                                        "jdbcUrl": [
                                            f"jdbc:mysql://{mysql_params.get('host', self.mysql_host)}:{mysql_params.get('port', self.mysql_port)}/{mysql_params.get('database', self.mysql_database)}"]
                                    }
                                ],
                                "fetchSize": mysql_params.get("fetchSize", 1024)
                            }
                        },
                        "writer": {
                            "name": "postgresqlwriter",
                            "parameter": {
                                "username": pg_params.get("user"),
                                "password": pg_params.get("password"),
                                "column": column_names,  # 列名列表（修复的核心部分）
                                "connection": [
                                    {
                                        "table": [table_name],
                                        "jdbcUrl": f"jdbc:postgresql://{pg_params.get('host')}:{pg_params.get('port')}/{pg_params.get('database')}"
                                    }
                                ],
                                "preSql": [f"TRUNCATE TABLE {table_name}"],
                                "postSql": [],
                                "batchSize": pg_params.get("batchSize", 1024)
                            }
                        }
                    }
                ],
                "setting": {
                    "speed": {
                        "channel": pg_params.get("channel", 3)
                    },
                    "errorLimit": {
                        "record": 0,
                        "percentage": 0.02
                    }
                }
            }
        }

        # 写入配置文件
        filename = os.path.join(output_dir, f"{table_name}_mysql2pg.json")
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(datax_config, f, ensure_ascii=False, indent=4)
            return filename
        except Exception as e:
            print(f"生成配置文件{filename}错误: {e}")
            return None

    def batch_generate_configs(
            self,
            tables: Optional[List[str]] = None,
            mysql_params: Dict = None,
            pg_params: Dict = None,
            output_dir: str = "datax_configs"
    ) -> int:
        """批量生成配置文件"""
        if not self.connection or not self.connection.open:
            print("未连接到数据库")
            return 0

        if not tables:
            tables = self.get_tables()

        if not tables:
            print("没有找到要处理的表")
            return 0

        if not mysql_params:
            mysql_params = {}
        if not pg_params:
            pg_params = {}

        success_count = 0

        for table in tables:
            print(f"正在处理表: {table}")
            columns = self.get_table_columns(table)
            if not columns:
                print(f"表{table}没有找到列信息，跳过")
                continue

            filename = self.generate_datax_config(
                table_name=table,
                columns=columns,
                mysql_params=mysql_params,
                pg_params=pg_params,
                output_dir=output_dir
            )

            if filename:
                print(f"配置文件已生成: {filename}")
                success_count += 1
            else:
                print(f"生成表{table}的配置文件失败")

        return success_count

    def close(self):
        """关闭数据库连接"""
        if self.connection and self.connection.open:
            self.connection.close()
            print("MySQL数据库连接已关闭")


if __name__ == "__main__":
    # MySQL数据库连接配置 - 请根据实际情况修改
    mysql_config = {
        "host": "localhost",
        "port": 3306,
        "user": "admin",
        "password": "Admin@123456",
        "database": "qianfeng",
        "fetchSize": 1024
    }

    # PostgreSQL数据库配置 - 请根据实际情况修改
    pg_config = {
        "host": "localhost",
        "port": 5432,
        "user": "admin",
        "password": "Admin@123456",
        "database": "qianfeng",
        "channel": 3,
        "batchSize": 1024
    }

    # 初始化转换器
    converter = MySQLToPostgreSQLDataX(
        mysql_host=mysql_config["host"],
        mysql_port=mysql_config["port"],
        mysql_user=mysql_config["user"],
        mysql_password=mysql_config["password"],
        mysql_database=mysql_config["database"]
    )

    if converter.connect():
        print(f"成功连接到MySQL数据库: {mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")

        # 可以指定要处理的表，如 ["table1", "table2"]，为None则处理所有表
        tables_to_process = None

        # 批量生成配置文件
        success_count = converter.batch_generate_configs(
            tables=tables_to_process,
            mysql_params=mysql_config,
            pg_params=pg_config,
            output_dir="mysql_to_pg_datax_configs"
        )

        print(f"批量处理完成，成功生成{success_count}个配置文件")
        converter.close()
    else:
        print("MySQL数据库连接失败，无法继续处理")
