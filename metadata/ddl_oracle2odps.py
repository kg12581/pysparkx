import cx_Oracle
import re
import os
from datetime import datetime

# Oracle到ODPS数据类型映射表
TYPE_MAPPING = {
    'VARCHAR2': 'STRING',
    'NVARCHAR2': 'STRING',
    'CHAR': 'STRING',
    'NCHAR': 'STRING',
    'NUMBER': 'BIGINT',  # 基础映射，会根据精度动态调整
    'INT': 'BIGINT',
    'INTEGER': 'BIGINT',
    'BINARY_INTEGER': 'BIGINT',
    'FLOAT': 'DOUBLE',
    'DOUBLE PRECISION': 'DOUBLE',
    'DATE': 'DATETIME',
    'TIMESTAMP': 'DATETIME',
    'TIMESTAMP(6)': 'DATETIME',
    'CLOB': 'STRING',
    'BLOB': 'BINARY',
    'RAW': 'BINARY'
}


def get_all_oracle_tables(conn, exclude_tables=None):
    """获取Oracle用户下所有表名（排除指定表）"""
    exclude = exclude_tables or []
    exclude_upper = [t.upper() for t in exclude]

    with conn.cursor() as cursor:
        cursor.execute("""
                       SELECT TABLE_NAME
                       FROM USER_TABLES
                       WHERE TABLE_NAME NOT IN (:exclude)
                       ORDER BY TABLE_NAME
                       """, exclude=exclude_upper)
        return [row[0].lower() for row in cursor.fetchall()]


def get_oracle_table_metadata(conn, table_name):
    """提取单个表的元数据（字段信息、注释等）"""
    metadata = {
        'columns': [],
        'comments': {},
        'table_comment': ''
    }

    with conn.cursor() as cursor:
        # 获取字段信息
        cursor.execute("""
                       SELECT COLUMN_NAME,
                              DATA_TYPE,
                              DATA_LENGTH,
                              DATA_PRECISION,
                              DATA_SCALE,
                              NULLABLE
                       FROM USER_TAB_COLUMNS
                       WHERE TABLE_NAME = :tbl
                       ORDER BY COLUMN_ID
                       """, tbl=table_name.upper())

        for col in cursor.fetchall():
            col_name, data_type, data_len, data_prec, data_scale, nullable = col
            metadata['columns'].append({
                'name': col_name.lower(),
                'oracle_type': data_type,
                'data_length': data_len,
                'data_precision': data_prec,
                'data_scale': data_scale,
                'nullable': nullable == 'Y'
            })

        # 获取字段注释
        cursor.execute("""
                       SELECT COLUMN_NAME, COMMENTS
                       FROM USER_COL_COMMENTS
                       WHERE TABLE_NAME = :tbl
                       """, tbl=table_name.upper())

        for col_name, comment in cursor.fetchall():
            metadata['comments'][col_name.lower()] = comment or ''

        # 获取表注释
        cursor.execute("""
                       SELECT COMMENTS
                       FROM USER_TAB_COMMENTS
                       WHERE TABLE_NAME = :tbl
                       """, tbl=table_name.upper())

        table_comment = cursor.fetchone()[0]
        metadata['table_comment'] = table_comment or ''

    return metadata


def oracle_type_to_odps(oracle_type, data_precision, data_scale):
    """转换Oracle数据类型到ODPS类型"""
    base_type = oracle_type.split('(')[0]

    # 特殊处理NUMBER类型
    if base_type == 'NUMBER':
        if data_scale is not None and data_scale > 0:
            return 'DOUBLE'
        elif data_precision is not None:
            return 'INT' if data_precision <= 10 else 'BIGINT'
        return 'BIGINT'

    return TYPE_MAPPING.get(base_type, 'STRING')


def generate_odps_ddl(table_name, metadata, partition_cols=None, bucket_info=None):
    """生成单个表的ODPS DDL语句"""
    ddl = []
    ddl.append(f"-- 表 {table_name} 的ODPS建表语句")
    ddl.append(f"CREATE TABLE IF NOT EXISTS {table_name} (")

    # 字段定义
    col_defs = []
    for col in metadata['columns']:
        col_name = col['name']
        odps_type = oracle_type_to_odps(
            col['oracle_type'],
            col['data_precision'],
            col['data_scale']
        )
        # 处理注释中的特殊字符
        comment = metadata['comments'].get(col_name, '').replace("'", "\\'").replace("\n", " ")
        col_def = f"  `{col_name}` {odps_type} COMMENT '{comment}'"
        col_defs.append(col_def)

    ddl.append(',\n'.join(col_defs))
    ddl.append(')')

    # 分区配置
    if partition_cols:
        partition_str = ', '.join([f"`{col}` STRING" for col in partition_cols])
        ddl.append(f"PARTITIONED BY ({partition_str})")

    # 分桶配置
    if bucket_info and bucket_info['num_buckets'] > 0 and bucket_info['bucket_cols']:
        bucket_cols = ', '.join([f"`{col}`" for col in bucket_info['bucket_cols']])
        ddl.append(f"CLUSTERED BY ({bucket_cols}) INTO {bucket_info['num_buckets']} BUCKETS")

    # 表注释和生命周期
    table_comment = metadata['table_comment'].replace("'", "\\'").replace("\n", " ")
    ddl.append(f"COMMENT '{table_comment}'")
    ddl.append("LIFECYCLE 30;")  # 可根据实际需求调整生命周期
    ddl.append("")  # 空行分隔不同表的DDL
    return '\n'.join(ddl)


def batch_generate_ddl(oracle_conn, output_dir, exclude_tables=None,
                       partition_cols=None, bucket_info=None):
    """批量生成所有表的DDL并保存到文件"""
    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 获取所有表名
    tables = get_all_oracle_tables(oracle_conn, exclude_tables)
    print(f"发现 {len(tables)} 个表，开始生成DDL...")

    # 生成汇总文件和单个表文件
    summary_path = os.path.join(output_dir, f"all_tables_ddl_{datetime.now().strftime('%Y%m%d')}.sql")
    with open(summary_path, 'w', encoding='utf-8') as summary_file:
        for i, table_name in enumerate(tables, 1):
            try:
                print(f"处理表 {i}/{len(tables)}: {table_name}")
                metadata = get_oracle_table_metadata(oracle_conn, table_name)
                ddl = generate_odps_ddl(
                    table_name=table_name,
                    metadata=metadata,
                    partition_cols=partition_cols,
                    bucket_info=bucket_info
                )

                # 写入汇总文件
                summary_file.write(ddl + '\n')

                # 单独保存每个表的DDL
                table_file = os.path.join(output_dir, f"{table_name}.sql")
                with open(table_file, 'w', encoding='utf-8') as f:
                    f.write(ddl)

            except Exception as e:
                print(f"处理表 {table_name} 失败: {str(e)}")
                continue

    print(f"所有DDL生成完成，汇总文件: {summary_path}")


def main():
    # 配置Oracle连接
    try:
        oracle_conn = cx_Oracle.connect(
            user="your_username",
            password="your_password",
            dsn="host:port/service_name"  # 例如："192.168.1.1:1521/orcl"
        )
    except Exception as e:
        print(f"Oracle连接失败: {str(e)}")
        return

    # 配置参数
    output_directory = "./odps_ddl_output"  # DDL输出目录
    exclude_tables = ["TEMP_TABLE", "LOG_TABLE"]  # 需要排除的表名

    # 分区配置（可根据实际表结构动态调整，这里示例为通用配置）
    partition_columns = ["dt"]  # 例如按日期分区

    # 分桶配置（可选，根据查询需求设置）
    bucket_config = {
        'num_buckets': 8,
        'bucket_cols': ["id"]  # 通常使用主键或高频查询字段
    }

    # 批量生成DDL
    batch_generate_ddl(
        oracle_conn=oracle_conn,
        output_dir=output_directory,
        exclude_tables=exclude_tables,
        partition_cols=partition_columns,
        bucket_info=bucket_config
    )

    oracle_conn.close()



"""
根据oracle元数据信息批量生成建表语句
pip install cx_Oracle
"""
if __name__ == "__main__":
    main()
