"""
# 定义运行环境
env {
  parallelism = 2
  job.mode = "STREAMING"
}
source {
  Kafka {
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
    format = text
    field_delimiter = "#"
    topic = "kafka2pg22"
    bootstrap.servers = "localhost:9092"
    kafka.config = {
      client.id = client_1
      max.poll.records = 500
      auto.offset.reset = "earliest"
      enable.auto.commit = "false"
    }
  }
}

sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432/test"
    driver = "org.postgresql.Driver"
    user = "admin"
    password = "Admin@123456"
    database = "test"              # 必须要加
    table = "public.test_table"
    generate_sink_sql = true
  }
}
"""