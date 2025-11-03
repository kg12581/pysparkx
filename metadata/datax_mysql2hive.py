import pymysql
import json
import os
from typing import List, Dict, Optional


class DatabaseConverter:
    def __init__(self, source_type: str, source_host: str, source_port: int, source_user: str,
                 source_password: str, source_database: str,
                 sink_type: str, sink_host: str, sink_port: int, sink_user: str,
                 sink_password: str, sink_database: str):
        self.source_type = source_type
        self.source_host = source_host
        self.source_port = source_port
        self.source_user = source_user
        self.source_password = source_password
        self.source_database = source_database
        self.sink_type = sink_type
        self.sink_host = sink_host
        self.sink_port = sink_port
        self.sink_user = sink_user
        self.sink_password = sink_password
        self.sink_database = sink_database
        self.source_connection = None
        self.type_mappings = {
           'mysql': {
                'hive': {
                    # 数值类型
                    'int': 'INT',
                    'tinyint': 'TINYINT',
                   'smallint': 'SMALLINT',
                   'mediumint': 'INT',
                    'bigint': 'BIGINT',
                    'float': 'FLOAT',
                    'double': 'DOUBLE',
                    'decimal': 'DECIMAL',
                    'numeric': 'DECIMAL',
                    # 字符串类型
                    'char': 'CHAR',
                    'varchar': 'VARCHAR',
                    'text': 'STRING',
                    'tinytext': 'STRING',
                   'mediumtext': 'STRING',
                    'longtext': 'STRING',
                    # 日期时间类型
                    'date': 'DATE',
                    'datetime': 'TIMESTAMP',
                    'timestamp': 'TIMESTAMP',
                    'time': 'TIME',
                    'year': 'INT',
                    # 二进制类型
                    'binary': 'BINARY',
                    'varbinary': 'BINARY',
                    'blob': 'BINARY',
                    'tinyblob': 'BINARY',
                   'mediumblob': 'BINARY',
                    'longblob': 'BINARY',
                    # 其他类型
                    'enum': 'STRING',
                   'set': 'STRING',
                    'bit': 'BOOLEAN'
                }
            }
        }

    def connect_source(self) -> bool:
        if self.source_type =='mysql':
            try:
                self.source_connection = pymysql.connect(
                    host=self.source_host,
                    port=self.source_port,
                    user=self.source_user,
                    password=self.source_password,
                    database=self.source_database,
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor
                )
                return True
            except pymysql.MySQLError as e:
                print(f"{self.source_type}数据库连接错误: {e}")
                return False
        else:
            print(f"暂不支持的源数据库类型: {self.source_type}")
            return False

    def get_tables(self) -> List[str]:
        if not self.source_connection or not self.source_connection.open:
            print("未连接到源数据库")
            return []

        tables = []
        if self.source_type =='mysql':
            try:
                with self.source_connection.cursor() as cursor:
                    cursor.execute(f"SHOW TABLES FROM {self.source_database}")
                    for row in cursor.fetchall():
                        tables.append(list(row.values())[0])
            except pymysql.MySQLError as e:
                print(f"获取表名错误: {e}")
        return tables

    def get_table_columns(self, table_name: str) -> List[Dict]:
        if not self.source_connection or not self.source_connection.open:
            print("未连接到源数据库")
            return []

        columns = []
        if self.source_type =='mysql':
            try:
                with self.source_connection.cursor() as cursor:
                    cursor.execute(f"DESCRIBE {table_name}")
                    for row in cursor.fetchall():
                        base_type = row['Type'].split('(')[0].lower()
                        target_type = self.type_mappings[self.source_type][self.sink_type].get(base_type, 'VARCHAR')
                        columns.append({
                            "name": row['Field'],
                            "source_type": row['Type'],
                            "sink_type": target_type
                        })
            except pymysql.MySQLError as e:
                print(f"获取表{table_name}的列信息错误: {e}")
        return columns

    def generate_datax_config(
            self,
            table_name: str,
            columns: List[Dict],
            output_dir: str = "datax_configs"
    ) -> Optional[str]:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        if self.sink_type == 'hive':
            datax_config = {
                "job": {
                    "content": [
                        {
                            "reader": {
                                "name": "mysqlreader",
                                "parameter": {
                                    "username": self.source_user,
                                    "password": self.source_password,
                                    "column": [col["name"] for col in columns],
                                    "connection": [
                                        {
                                            "table": [table_name],
                                            "jdbcUrl": [f"jdbc:mysql://{self.source_host}:{self.source_port}/{self.source_database}"]
                                        }
                                    ]
                                }
                            },
                            "writer": {
                                "name": "hdfswriter",
                                "parameter": {
                                    "defaultFS": f"hdfs://{self.sink_host}:{self.sink_port}",
                                    "fileType": "text",
                                    "path": f"/user/hive/warehouse/{self.sink_database}.db/{table_name}",
                                    "writeMode": "truncate",
                                    "fieldDelimiter": ",",
                                    "fileName": f"{table_name}",
                                    "column": [{"name": col["name"], "type": col["sink_type"]} for col in columns]
                                }
                            }
                        }
                    ],
                    "setting": {
                        "speed": {
                            "channel": 3
                        },
                        "errorLimit": {
                            "record": 0,
                            "percentage": 0.02
                        }
                    }
                }
            }
        elif self.sink_type == 'doris':
            column_mappings = [{"name": col["name"]} for col in columns]
            datax_config = {
                "job": {
                    "content": [
                        {
                            "reader": {
                                "name": "mysqlreader",
                                "parameter": {
                                    "username": self.source_user,
                                    "password": self.source_password,
                                    "column": [col["name"] for col in columns],
                                    "connection": [
                                        {
                                            "table": [table_name],
                                            "jdbcUrl": [f"jdbc:mysql://{self.source_host}:{self.source_port}/{self.source_database}"]
                                        }
                                    ]
                                }
                            },
                            "writer": {
                                "name": "doriswriter",
                                "parameter": {
                                    "username": self.sink_user,
                                    "password": self.sink_password,
                                    "column": column_mappings,
                                    "connection": [
                                        {
                                            "table": [table_name],
                                            "jdbcUrl": f"jdbc:doris://{self.sink_host}:{self.sink_port}/{self.sink_database}"
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    "setting": {
                        "speed": {
                            "channel": 3
                        },
                        "errorLimit": {
                            "record": 0,
                            "percentage": 0.02
                        }
                    }
                }
            }
        else:
            print(f"暂不支持的目标数据库类型: {self.sink_type}")
            return None

        filename = os.path.join(output_dir, f"{table_name}_{self.source_type}2{self.sink_type}.json")
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
            output_dir: str = "datax_configs"
    ) -> int:
        if not self.source_connection or not self.source_connection.open:
            print("未连接到源数据库")
            return 0

        if not tables:
            tables = self.get_tables()

        if not tables:
            print("没有找到要处理的表")
            return 0

        success_count = 0

        for table in tables:
            # print(f"正在处理表: {table}")
            columns = self.get_table_columns(table)
            if not columns:
                print(f"表{table}没有找到列信息，跳过")
                continue

            filename = self.generate_datax_config(
                table_name=table,
                columns=columns,
                output_dir=output_dir
            )

            if filename:
                print(f"配置文件已生成: {filename}")
                success_count += 1
            else:
                print(f"生成表{table}的配置文件失败")

        return success_count

    def close(self):
        if self.source_connection and self.source_connection.open:
            self.source_connection.close()
            print(f"{self.source_type}数据库连接已关闭")


if __name__ == "__main__":
    # MySQL到Hive的转换示例 - 请根据实际情况修改
    source_config = {
        "type": "mysql",
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "Admin@123456",
        "database": "qianfeng"
    }
    sink_config = {
        "type": "hive",
        "host": "localhost",
        "port": 8020,  # HDFS Namenode端口，根据实际情况修改
        "user": "kgt",
        "password": "",
        "database": "qianfeng"
    }

    converter = DatabaseConverter(
        source_type=source_config["type"],
        source_host=source_config["host"],
        source_port=source_config["port"],
        source_user=source_config["user"],
        source_password=source_config["password"],
        source_database=source_config["database"],
        sink_type=sink_config["type"],
        sink_host=sink_config["host"],
        sink_port=sink_config["port"],
        sink_user=sink_config["user"],
        sink_password=sink_config["password"],
        sink_database=sink_config["database"]
    )

    if converter.connect_source():
        print(f"成功连接到{source_config['type']}数据库: {source_config['host']}:{source_config['port']}/{source_config['database']}")

        tables_to_process = None
        success_count = converter.batch_generate_configs(
            tables=tables_to_process,
            output_dir="mysql_to_hive_datax_configs"
        )

        print(f"批量处理完成，成功生成{success_count}个配置文件")

        converter.close()
    else:
        print(f"{source_config['type']}数据库连接失败，无法继续处理")
