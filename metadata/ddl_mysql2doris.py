import pymysql
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Tuple


class MySQL2Doris:
    def __init__(
            self,
            # MySQL 配置
            mysql_host: str,
            mysql_port: int,
            mysql_user: str,
            mysql_password: str,
            mysql_db: str,
            # Doris 配置（兼容MySQL协议）
            doris_host: str,
            doris_port: int,
            doris_user: str,
            doris_password: str,
            doris_db: str,
            doris_schema: str = "default"
    ):
        # 初始化MySQL连接
        self.mysql_conn = self._connect_mysql(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            db=mysql_db
        )

        # 初始化Doris连接（使用sqlalchemy）
        self.doris_engine = self._connect_doris(
            host=doris_host,
            port=doris_port,
            user=doris_user,
            password=doris_password,
            db=doris_db
        )

        self.doris_db = doris_db
        self.doris_schema = doris_schema

        # MySQL到Doris的数据类型映射表
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
            'char': 'CHAR',
            'varchar': 'VARCHAR',
            'text': 'TEXT',
            'mediumtext': 'TEXT',
            'longtext': 'TEXT',
            # 日期时间类型
            'date': 'DATE',
            'time': 'TIME',
            'datetime': 'DATETIME',
            'timestamp': 'DATETIME',
            # 其他类型
            'boolean': 'BOOLEAN',
            'json': 'JSONB'
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

    def _connect_doris(self, host: str, port: int, user: str, password: str, db: str) -> create_engine:
        """连接Doris数据库（使用sqlalchemy）"""
        try:
            # Doris兼容MySQL协议，使用mysql+pymysql驱动
            connection_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
            engine = create_engine(connection_url)
            # 测试连接
            with engine.connect():
                print("✅ 成功连接到Doris数据库")
                return engine
        except Exception as e:
            raise RuntimeError(f"❌ Doris连接失败: {str(e)}") from e

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
                               """, (self.mysql_conn.db,))
                tables = [row['table_name'] for row in cursor.fetchall()]
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

    def map_mysql_type_to_doris(self, mysql_type: str) -> str:
        """将MySQL数据类型映射为Doris类型"""
        # 提取基础类型（去除括号里的参数）
        base_type = mysql_type.split('(')[0].lower()
        if base_type in self.type_mapping:
            return self.type_mapping[base_type]
        # 处理未映射的类型，默认使用VARCHAR
        print(f"⚠️ 未找到 {mysql_type} 的映射关系，使用VARCHAR替代")
        return "VARCHAR"

    def generate_doris_create_sql(self, table_name: str, table_schema: List[Dict]) -> str:
        """生成Doris表的创建SQL"""
        if not table_schema:
            raise ValueError(f"表 {table_name} 没有字段信息，无法生成建表语句")

        # 构建字段定义
        columns = []
        for field in table_schema:
            field_name = field['column_name']
            mysql_type = field['data_type']
            doris_type = self.map_mysql_type_to_doris(mysql_type)

            # 保留原始类型的参数（如varchar(255)）
            if '(' in mysql_type:
                doris_type += mysql_type[mysql_type.find('('):]

            # 处理非空约束
            nullable = "NULL" if field['is_nullable'] else "NOT NULL"

            # 处理默认值
            default_clause = ""
            if field['column_default'] is not None:
                # 字符串类型默认值需要加引号
                if mysql_type.startswith(('char', 'varchar', 'text', 'date', 'time', 'datetime', 'timestamp')):
                    default_clause = f" DEFAULT '{field['column_default']}'"
                else:
                    default_clause = f" DEFAULT {field['column_default']}"

            # 处理注释（转义单引号）
            comment = field['column_comment'].replace("'", "\\'")
            comment_clause = f" COMMENT '{comment}'" if comment else ""

            # 拼接字段定义
            columns.append(f"`{field_name}` {doris_type} {nullable}{default_clause}{comment_clause}")

        # 生成建表SQL（Doris特有语法）
        create_sql = f"""
CREATE TABLE IF NOT EXISTS `{self.doris_schema}`.`{table_name}` (
    {',    '.join(columns)}
) ENGINE=OLAP
DUPLICATE KEY(`{table_schema[0]['column_name']}`)  # 使用第一个字段作为排序键
COMMENT '从MySQL表 {self.mysql_conn.db}.{table_name} 同步'
DISTRIBUTED BY HASH(`{table_schema[0]['column_name']}`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  # 副本数，根据集群规模调整
    "storage_medium" = "HDD"   # 存储介质，可选SSD/HDD
);
        """.strip()

        return create_sql

    def create_doris_table(self, create_sql: str) -> bool:
        """执行SQL创建Doris表"""
        try:
            with self.doris_engine.connect() as conn:
                conn.execute(create_sql)
                conn.commit()
                print(f"✅ 执行建表SQL成功:\n{create_sql[:200]}...")  # 只显示前200字符
                return True
        except SQLAlchemyError as e:
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
                create_sql = self.generate_doris_create_sql(table, schema)

                # 执行创建表
                if self.create_doris_table(create_sql):
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


if __name__ == "__main__":
    # 配置信息（请根据实际环境修改）
    config = {
        # MySQL配置
        "mysql": {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "your_mysql_password",
            "db": "your_mysql_database"
        },
        # Doris配置（兼容MySQL协议，端口默认9030）
        "doris": {
            "host": "localhost",
            "port": 9030,
            "user": "root",
            "password": "your_doris_password",
            "db": "your_doris_database",
            "schema": "default"
        }
    }

    # 创建同步工具实例
    sync_tool = MySQL2Doris(
        mysql_host=config["mysql"]["host"],
        mysql_port=config["mysql"]["port"],
        mysql_user=config["mysql"]["user"],
        mysql_password=config["mysql"]["password"],
        mysql_db=config["mysql"]["db"],
        doris_host=config["doris"]["host"],
        doris_port=config["doris"]["port"],
        doris_user=config["doris"]["user"],
        doris_password=config["doris"]["password"],
        doris_db=config["doris"]["db"],
        doris_schema=config["doris"]["schema"]
    )

    try:
        # 批量同步所有表
        # 如需指定表同步，可以传入参数，例如：["table1", "table2"]
        sync_tool.batch_sync_tables()
    finally:
        # 关闭连接
        sync_tool.close_connections()
