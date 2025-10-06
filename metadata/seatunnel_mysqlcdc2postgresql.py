"""

env {
  parallelism = 1
  job.mode = "STREAMING"  # 流模式，必须开启（监听增量数据）
  checkpoint.interval = 10000  # 10秒一次checkpoint，确保数据不丢失
}

source {
  # 关键1：明确声明 MySQL-CDC 连接器类型
  MySQL-CDC {
    connector = "mysql-cdc"  # 必须添加！告诉SeaTunnel这是CDC源，而非普通JDBC源
    url = "jdbc:mysql://localhost:3306/qianfeng?useSSL=false&serverTimezone=Asia/Shanghai"  # 补充SSL关闭和时区（避免连接报错）
    username = "root"
    password = ""  # 若MySQL root无密码，保持空字符串（需确保MySQL允许空密码登录）
    table-names = ["qianfeng.question_difficulty"]  # 同步的表（库名.表名）
    server-id = 5400  # 唯一标识（必须与MySQL主库server-id不重复，主库通常为1）
    server-time-zone = "Asia/Shanghai"  # 与MySQL时区一致（避免binlog时间解析错误）
    startup.mode = INITIAL

    # 可选优化：CDC增量捕获参数
    binlog.fetch.size = 1024  # 每次拉取binlog的大小（避免内存溢出）
    scan.incremental.snapshot.chunk.size = 8192  # 全量同步时分块大小（优化性能）
  }
}

sink {
  # 关键2：配置JDBC Sink为流模式适配（支持增量INSERT事件）
  Jdbc {
    connector = "jdbc"  # 明确连接器类型
    url = "jdbc:postgresql://localhost:5432/qianfeng?useSSL=false"  # 补充SSL关闭（避免PostgreSQL连接报错）
    driver = "org.postgresql.Driver"  # PostgreSQL驱动（正确）
    user = "admin"
    password = "Admin@123456"
    database = "qianfeng"
    table = "public.question_difficulty"  # 目标表（PostgreSQL默认schema为public）
    write.mode = "append"  # 增量插入模式（仅新增数据，适合捕获INSERT事件；若需更新用"upsert"）
    batch.size = 100  # 批量写入大小（优化性能，避免频繁提交）
    batch.flush.interval = 3000  # 3秒内若批量未达100条，也触发写入（避免数据延迟）

    # 若后续需支持UPDATE/DELETE，需开启以下配置（当前仅捕获INSERT可省略）
    primary.key = "id"  # 目标表主键（与MySQL源表一致，"upsert"模式必须）
    generate_sink_sql = true  # 自动生成写入SQL（可选，手动配置sql时关闭）
  }
}

"""