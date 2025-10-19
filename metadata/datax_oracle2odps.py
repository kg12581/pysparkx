import cx_Oracle
import json
import os
from datetime import datetime

# Oracle到ODPS数据类型映射（DataX内部转换用）
DATA_TYPE_MAPPING = {
    'VARCHAR2': 'string',
    'NVARCHAR2': 'string',
    'CHAR': 'string',
    'NCHAR': 'string',
    'NUMBER': 'long',  # 整数默认映射
    'NUMBER_DECIMAL': 'double',  # 带小数的NUMBER
    'INT': 'long',
    'INTEGER': 'long',
    'BINARY_INTEGER': 'long',
    'FLOAT': 'double',
    'DOUBLE PRECISION': 'double',
    'DATE': 'date',
    'TIMESTAMP': 'date',
    'TIMESTAMP(6)': 'date',
    'CLOB': 'string',
    'BLOB': 'bytes',
    'RAW': 'bytes'
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


def get_oracle_table_columns(conn, table_name):
    """获取表字段信息（用于DataX字段映射）"""
    columns = []
    with conn.cursor() as cursor:
        cursor.execute("""
                       SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
                       FROM USER_TAB_COLUMNS
                       WHERE TABLE_NAME = :tbl
                       ORDER BY COLUMN_ID
                       """, tbl=table_name.upper())

        for col in cursor.fetchall():
            col_name, data_type, data_prec, data_scale = col
            # 处理NUMBER类型区分整数和小数
            if data_type == 'NUMBER' and data_scale is not None and data_scale > 0:
                mapped_type = DATA_TYPE_MAPPING['NUMBER_DECIMAL']
            else:
                base_type = data_type.split('(')[0]
                mapped_type = DATA_TYPE_MAPPING.get(base_type, 'string')

            columns.append({
                'name': col_name.lower(),
                'type': mapped_type
            })
    return columns


def generate_datax_config(
        table_name,
        oracle_columns,
        oracle_config,
        odps_config,
        incremental_column=None  # 增量同步字段（如update_time）
):
    """生成单个表的DataX配置"""
    # 构建字段映射
    column_mappings = []
    for col in oracle_columns:
        column_mappings.append({
            "name": col['name'],
            "type": col['type']
        })

    # 读取器配置（Oracle）
    reader = {
        "name": "oraclereader",
        "parameter": {
            "username": oracle_config['user'],
            "password": oracle_config['password'],
            "connection": [
                {
                    "querySql": [
                        f"SELECT {', '.join([c['name'] for c in oracle_columns])} FROM {table_name.upper()} WHERE 1=1 {'' if not incremental_column else f'AND {incremental_column} > ${{last_time}}'}"],
                    "jdbcUrl": [oracle_config['jdbc_url']]
                }
            ],
            "column": [{"name": col['name'], "type": col['type']} for col in oracle_columns]
        }
    }

    # 写入器配置（ODPS）
    writer = {
        "name": "odpswriter",
        "parameter": {
            "accessId": odps_config['access_id'],
            "accessKey": odps_config['access_key'],
            "project": odps_config['project'],
            "endpoint": odps_config['endpoint'],
            "table": table_name,
            "column": [col['name'] for col in oracle_columns],
            "writeMode": "append"  # 可根据需求改为overwrite
        }
    }

    # 完整配置
    datax_config = {
        "job": {
            "content": [
                {
                    "reader": reader,
                    "writer": writer
                }
            ],
            "setting": {
                "speed": {
                    "channel": 3  # 并发通道数
                },
                "errorLimit": {
                    "record": 0,
                    "percentage": 0.02
                }
            }
        }
    }

    return datax_config


def batch_generate_datax(
        oracle_conn,
        output_dir,
        oracle_config,
        odps_config,
        exclude_tables=None,
        incremental_tables=None  # 增量表配置 {table_name: incremental_column}
):
    """批量生成所有表的DataX配置"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    tables = get_all_oracle_tables(oracle_conn, exclude_tables)
    incremental_tables = incremental_tables or {}
    print(f"发现 {len(tables)} 个表，开始生成DataX配置...")

    # 生成汇总清单
    summary_path = os.path.join(output_dir, f"datax_config_list_{datetime.now().strftime('%Y%m%d')}.txt")
    with open(summary_path, 'w', encoding='utf-8') as summary_file:
        for i, table_name in enumerate(tables, 1):
            try:
                print(f"处理表 {i}/{len(tables)}: {table_name}")
                columns = get_oracle_table_columns(oracle_conn, table_name)

                # 检查是否为增量表
                incremental_column = incremental_tables.get(table_name)

                # 生成配置
                datax_config = generate_datax_config(
                    table_name=table_name,
                    oracle_columns=columns,
                    oracle_config=oracle_config,
                    odps_config=odps_config,
                    incremental_column=incremental_column
                )

                # 保存配置文件
                config_path = os.path.join(output_dir, f"{table_name}_datax.json")
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(datax_config, f, ensure_ascii=False, indent=2)

                # 写入汇总清单
                summary_file.write(f"{table_name}: {config_path}\n")

            except Exception as e:
                print(f"处理表 {table_name} 失败: {str(e)}")
                continue

    print(f"所有DataX配置生成完成，汇总清单: {summary_path}")


def main():
    # 配置Oracle连接
    try:
        oracle_conn = cx_Oracle.connect(
            user="your_username",
            password="your_password",
            dsn="host:port/service_name"
        )
    except Exception as e:
        print(f"Oracle连接失败: {str(e)}")
        return

    # 数据源配置
    oracle_config = {
        "user": "your_username",
        "password": "your_password",
        "jdbc_url": "jdbc:oracle:thin:@host:port:service_name"
    }

    # ODPS配置
    odps_config = {
        "access_id": "your_odps_access_id",
        "access_key": "your_odps_access_key",
        "project": "your_odps_project",
        "endpoint": "http://service.odps.aliyun.com/api"
    }

    # 输出目录
    output_directory = "./datax_configs"

    # 排除表配置
    # exclude_tables = ["TEMP_TABLE", "LOG_TABLE"]

    # # 增量表配置（表名: 增量字段）
    # incremental_tables = {
    #     "user_info": "update_time",
    #     "order_data": "create_time"
    # }

    # 批量生成配置
    batch_generate_datax(
        oracle_conn=oracle_conn,
        output_dir=output_directory,
        oracle_config=oracle_config,
        odps_config=odps_config,
        # exclude_tables=exclude_tables,
        # incremental_tables=incremental_tables
    )

    oracle_conn.close()

"""
批量生成datax文件 需要验证 可能有问题
"""
if __name__ == "__main__":
    main()
