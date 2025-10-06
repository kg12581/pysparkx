from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,  # 插入事件
    UpdateRowsEvent,  # 更新事件
    DeleteRowsEvent  # 删除事件
)


"""
实现监控mysql指定库的表字段的变更信息，并发送群钉钉机器人消息
source: mysql cdc,postgresql cdc,oracle cdc,sql server cdc,Opengauss CDC,MongoDB CDC
sink: 邮件,钉钉机器人
"""



# 1. MySQL 连接配置
mysql_settings = {
    "host": "localhost",  # MySQL 地址
    "port": 3306,  # 端口
    "user": "root",  # 之前创建的 CDC 专用用户
    "password": ""  # 用户密码
}

# 2. 初始化 Binlog 流读取器
# 关键参数说明：
# - server_id：必须与 MySQL 配置的 server-id 不同（避免冲突，设为 2）
# - blocking：True 表示持续监听（阻塞模式，实时捕获变更）
# - resume_stream：True 表示从当前 binlog 位置继续监听（避免重复消费）
# - only_events：指定只监听插入/更新/删除事件
stream = BinLogStreamReader(
    connection_settings=mysql_settings,
    server_id=2,
    blocking=True,
    resume_stream=True,
    only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
    # 仅监听 test_db 数据库的 user 表（可选，缩小监控范围）
    only_schemas=["qianfeng"],
    only_tables=["question_difficulty"]
)

# 3. 持续监听并处理变更事件
try:
    print("开始监听 MySQL CDC 数据...")
    for binlog_event in stream:
        # 遍历事件中的每一行变更
        for row in binlog_event.rows:
            # 解析事件类型和数据
            if isinstance(binlog_event, WriteRowsEvent):
                event_type = "INSERT"  # 插入操作
                data = row["values"]  # 插入的新数据（字典格式，key=字段名，value=值）
            elif isinstance(binlog_event, UpdateRowsEvent):
                event_type = "UPDATE"  # 更新操作
                old_data = row["before_values"]  # 更新前的数据
                new_data = row["after_values"]  # 更新后的数据
                data = {"old": old_data, "new": new_data}
            elif isinstance(binlog_event, DeleteRowsEvent):
                event_type = "DELETE"  # 删除操作
                data = row["values"]  # 删除前的数据（用于追溯）

            # 打印变更详情
            print(f"事件类型: {event_type}")
            print(f"数据库: {binlog_event.schema}")
            print(f"表名: {binlog_event.table}")
            print(f"变更数据: {data}")
            print("-" * 50)

except KeyboardInterrupt:
    print("手动停止监听")
finally:
    # 关闭流连接
    stream.close()