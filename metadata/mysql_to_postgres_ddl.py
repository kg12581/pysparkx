import pymysql
import psycopg2
from pymysql import OperationalError, ProgrammingError
from psycopg2 import OperationalError as PgOperationalError
from psycopg2 import ProgrammingError as PgProgrammingError
import re

"""
实现mysql表的元数据信息批量创建目标数据库postgresql的ddl
source: mysql,postgresql,oracle
sink:postgresql,hive,doris
"""

class MySQLToPostgresMigrator:
    def __init__(self, mysql_config, pg_config):
        """初始化数据库连接参数"""
        # MySQL配置
        self.mysql_host = mysql_config['host']
        self.mysql_user = mysql_config['user']
        self.mysql_password = mysql_config['password']
        self.mysql_db = mysql_config['db']
        self.mysql_port = mysql_config.get('port', 3306)
        self.mysql_charset = mysql_config.get('charset', 'utf8mb4')

        # PostgreSQL配置
        self.pg_host = pg_config['host']
        self.pg_user = pg_config['user']
        self.pg_password = pg_config['password']
        self.pg_db = pg_config['db']
        self.pg_port = pg_config.get('port', 5432)

        # 数据库连接
        self.mysql_conn = None
        self.pg_conn = None

        # MySQL到PostgreSQL的数据类型映射
        self.type_mapping = {
            # 数值类型
            'int': 'INTEGER',
            'tinyint': 'SMALLINT',
            'smallint': 'SMALLINT',
            'mediumint': 'INTEGER',
            'bigint': 'BIGINT',
            'float': 'REAL',
            'double': 'DOUBLE PRECISION',
            'decimal': 'NUMERIC',
            'numeric': 'NUMERIC',

            # 字符串类型
            'char': 'CHAR',
            'varchar': 'VARCHAR',
            'text': 'TEXT',
            'tinytext': 'TEXT',
            'mediumtext': 'TEXT',
            'longtext': 'TEXT',

            # 二进制类型
            'blob': 'BYTEA',
            'tinyblob': 'BYTEA',
            'mediumblob': 'BYTEA',
            'longblob': 'BYTEA',

            # 日期时间类型
            'date': 'DATE',
            'datetime': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            'time': 'TIME',
            'year': 'INTEGER',  # PostgreSQL没有year类型，用整数代替

            # 其他类型
            'bit': 'BIT',
            'boolean': 'BOOLEAN',
            'enum': 'VARCHAR',  # 将枚举转换为varchar
            'set': 'VARCHAR',  # 将set转换为varchar
            'json': 'JSONB'  # PostgreSQL推荐使用JSONB
        }

    def connect_mysql(self):
        """建立MySQL数据库连接"""
        try:
            self.mysql_conn = pymysql.connect(
                host=self.mysql_host,
                user=self.mysql_user,
                password=self.mysql_password,
                database=self.mysql_db,
                port=self.mysql_port,
                charset=self.mysql_charset
            )
            print("MySQL连接成功")
            return True
        except OperationalError as e:
            print(f"MySQL连接失败: {e}")
            return False

    def connect_postgres(self):
        """建立PostgreSQL数据库连接"""
        try:
            self.pg_conn = psycopg2.connect(
                host=self.pg_host,
                user=self.pg_user,
                password=self.pg_password,
                dbname=self.pg_db,
                port=self.pg_port
            )
            # 设置自动提交，避免事务阻塞
            self.pg_conn.autocommit = True
            print("PostgreSQL连接成功")
            return True
        except PgOperationalError as e:
            print(f"PostgreSQL连接失败: {e}")
            return False

    def close_connections(self):
        """关闭所有数据库连接"""
        if self.mysql_conn and self.mysql_conn.open:
            self.mysql_conn.close()
        if self.pg_conn and not self.pg_conn.closed:
            self.pg_conn.close()
        print("所有数据库连接已关闭")

    def get_tables(self):
        """获取MySQL数据库中所有表"""
        if not self.mysql_conn or not self.mysql_conn.open:
            print("请先建立有效的MySQL连接")
            return []

        try:
            with self.mysql_conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                return [table[0] for table in cursor.fetchall()]
        except ProgrammingError as e:
            print(f"获取表列表失败: {e}")
            return []

    def get_table_columns(self, table_name):
        """获取指定表的列信息"""
        if not self.mysql_conn or not self.mysql_conn.open:
            print("请先建立有效的MySQL连接")
            return []

        try:
            with self.mysql_conn.cursor() as cursor:
                cursor.execute(f"DESCRIBE {table_name}")
                columns = []
                for column in cursor.fetchall():
                    columns.append({
                        'name': column[0],
                        'type': column[1],
                        'nullable': column[2] == 'YES',
                        'key': column[3],
                        'default': column[4],
                        'extra': column[5]
                    })
                return columns
        except ProgrammingError as e:
            print(f"获取表 {table_name} 列信息失败: {e}")
            return []

    def get_primary_keys(self, table_name):
        """获取指定表的主键信息"""
        if not self.mysql_conn or not self.mysql_conn.open:
            print("请先建立有效的MySQL连接")
            return []

        try:
            with self.mysql_conn.cursor() as cursor:
                cursor.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name = 'PRIMARY'")
                indexes = list(cursor.fetchall())
                # 按Seq_in_index排序确保主键顺序正确
                indexes.sort(key=lambda x: x[3])
                return [idx[4] for idx in indexes]
        except ProgrammingError as e:
            print(f"获取表 {table_name} 主键失败: {e}")
            return []

    def get_indexes(self, table_name):
        """获取指定表的索引信息（不包括主键）"""
        if not self.mysql_conn or not self.mysql_conn.open:
            print("请先建立有效的MySQL连接")
            return []

        try:
            with self.mysql_conn.cursor() as cursor:
                cursor.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name != 'PRIMARY'")
                indexes = cursor.fetchall()

                index_dict = {}
                for idx in indexes:
                    key_name = idx[2]
                    column_name = idx[4]
                    non_unique = idx[1] == 1

                    if key_name not in index_dict:
                        index_dict[key_name] = {
                            'columns': [],
                            'unique': not non_unique
                        }
                    index_dict[key_name]['columns'].append(column_name)

                return index_dict
        except ProgrammingError as e:
            print(f"获取表 {table_name} 索引失败: {e}")
            return []

    def convert_data_type(self, mysql_type):
        """将MySQL数据类型转换为PostgreSQL数据类型"""
        # 提取类型名称（去掉括号及内容）
        base_type = re.match(r'^[a-zA-Z]+', mysql_type).group(0).lower()

        # 特殊处理带参数的类型
        if base_type in ['char', 'varchar']:
            # 提取长度
            match = re.search(r'\((\d+)\)', mysql_type)
            length = match.group(1) if match else '255'
            return f"{self.type_mapping[base_type]}({length})"

        elif base_type in ['decimal', 'numeric', 'float', 'double']:
            # 提取精度和小数位数
            match = re.search(r'\((\d+,\s*\d+)\)', mysql_type)
            if match:
                return f"{self.type_mapping[base_type]}({match.group(1)})"
            return self.type_mapping[base_type]

        elif base_type == 'enum':
            # 提取枚举值
            match = re.search(r'\((.*)\)', mysql_type)
            if match:
                return f"""VARCHAR(255) CHECK ({match.group(1).replace("'", '"')})"""
            return 'VARCHAR(255)'

        # 使用默认映射
        return self.type_mapping.get(base_type, 'VARCHAR(255)')

    def generate_ddl(self, table_name):
        """生成PostgreSQL的CREATE TABLE语句"""
        columns = self.get_table_columns(table_name)
        if not columns:
            return None

        primary_keys = self.get_primary_keys(table_name)
        indexes = self.get_indexes(table_name)

        ddl_lines = [f"CREATE TABLE {table_name} ("]

        # 添加列定义
        for col in columns:
            pg_type = self.convert_data_type(col['type'])
            line = f"""    "{col['name']}" {pg_type}"""

            # 处理NOT NULL
            if not col['nullable']:
                line += " NOT NULL"

            # 处理默认值
            if col['default'] is not None:
                # 处理特殊默认值
                if col['default'].upper() in ['CURRENT_TIMESTAMP', 'NOW()']:
                    line += " DEFAULT CURRENT_TIMESTAMP"
                elif col['default'].upper() == 'NULL':
                    line += " DEFAULT NULL"
                else:
                    # 处理字符串默认值
                    if col['type'].lower().startswith(('char', 'varchar', 'text', 'date', 'datetime')):
                        line += f" DEFAULT '{col['default']}'"
                    else:
                        line += f" DEFAULT {col['default']}"

            # 处理自增（MySQL的AUTO_INCREMENT对应PostgreSQL的SERIAL或IDENTITY）
            if 'auto_increment' in col['extra'].lower():
                # 对于PostgreSQL 10+，推荐使用IDENTITY
                line += " GENERATED ALWAYS AS IDENTITY"

            ddl_lines.append(line + ",")

        # 添加主键约束
        if primary_keys:
            pk_columns = ", ".join(primary_keys)
            ddl_lines.append(f"    PRIMARY KEY ({pk_columns}),")

        # 移除最后一个逗号
        if ddl_lines[-1].endswith(','):
            ddl_lines[-1] = ddl_lines[-1][:-1]

        ddl_lines.append(");")

        # 添加索引
        index_ddls = []
        for idx_name, idx_info in indexes.items():
            unique = "UNIQUE" if idx_info['unique'] else ""
            columns = ", ".join(idx_info['columns'])
            index_ddls.append(f"CREATE {unique} INDEX {idx_name} ON {table_name} ({columns});")

        return "\n".join(ddl_lines) + "\n" + "\n".join(index_ddls)

    def execute_ddl(self, ddl, table_name):
        """在PostgreSQL中执行DDL语句"""
        if not self.pg_conn or self.pg_conn.closed:
            print("请先建立有效的PostgreSQL连接")
            return False

        try:
            with self.pg_conn.cursor() as cursor:
                # 先检查表是否已存在，如果存在则删除（可选操作）
                # cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")

                # 执行建表语句
                cursor.execute(ddl)
                print(f"表 {table_name} 创建成功")
                return True
        except PgProgrammingError as e:
            print(f"执行DDL失败 (表 {table_name}): {e}")
            # 回滚事务
            self.pg_conn.rollback()
            return False

    def migrate_all_tables(self, output_file=None):
        """迁移所有表"""
        # 检查连接
        if not self.mysql_conn or not self.mysql_conn.open:
            print("MySQL连接未建立")
            return

        if not self.pg_conn or self.pg_conn.closed:
            print("PostgreSQL连接未建立")
            return

        tables = self.get_tables()
        if not tables:
            print("没有找到表")
            return

        all_ddl = []
        success_count = 0
        fail_count = 0

        for table in tables:
            print(f"\n处理表: {table}")

            # 生成DDL
            ddl = self.generate_ddl(table)
            if not ddl:
                print(f"生成表 {table} 的DDL失败")
                fail_count += 1
                continue

            all_ddl.append(ddl)

            # 执行DDL
            if self.execute_ddl(ddl, table):
                success_count += 1
            else:
                fail_count += 1

        # 保存DDL到文件
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("\n\n".join(all_ddl))
            print(f"\n所有DDL语句已写入文件: {output_file}")

        print(f"\n迁移完成 - 成功: {success_count}, 失败: {fail_count}")


# 使用示例
if __name__ == "__main__":
    # 配置数据库连接信息
    mysql_config = {
        'host': 'localhost',
        'user': 'admin',
        'password': 'Admin@123456',
        'db': 'qianfeng',
        'port': 3306
    }

    pg_config = {
        'host': 'localhost',
        'user': 'admin',
        'password': 'Admin@123456',
        'db': 'qianfeng',
        'port': 5432
    }

    # 创建迁移工具实例
    migrator = MySQLToPostgresMigrator(mysql_config, pg_config)

    # 连接数据库
    if migrator.connect_mysql() and migrator.connect_postgres():
        # 执行迁移
        migrator.migrate_all_tables("generated_postgres_ddl.sql")

    # 关闭连接
    migrator.close_connections()
