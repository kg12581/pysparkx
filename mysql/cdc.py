# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import StreamTableEnvironment, EnvironmentSettings
#
#
# def read_mysql_cdc():
#     # 创建执行环境
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)
#
#     env.add_jars(
#         "https://repo1.maven.org/maven2/com/ververica/flink-connector-mysql-cdc/2.4.1/flink-connector-mysql-cdc-2.4.1.jar"
#     )
#
#     # 配置表环境
#     settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
#     t_env = StreamTableEnvironment.create(env, environment_settings=settings)
#
#     # 配置MySQL CDC连接器
#     source_ddl = """
#                  CREATE TABLE mysql_cdc_source \
#                  ( \
#                      id          INT, \
#                      name        STRING, \
#                      price       DECIMAL(10, 2), \
#                      update_time TIMESTAMP(3), \
#                      PRIMARY KEY (id) NOT ENFORCED
#                  ) WITH (
#                        'connector' = 'mysql-cdc',
#                        'hostname' = 'localhost',
#                        'port' = '3306',
#                        'username' = 'root',
#                        'password' = '123456',
#                        'database-name' = 'test_db',
#                        'table-name' = 'products',
#                        'server-time-zone' = 'UTC'
#                        ) \
#                  """
#
#     # 执行DDL创建源表
#     t_env.execute_sql(source_ddl)
#
#     # 读取CDC数据并打印
#     t_env.execute_sql("SELECT * FROM mysql_cdc_source").print()
#
#     # 执行作业
#     env.execute("MySQL CDC Reader")
#
#
# if __name__ == "__main__":
#     read_mysql_cdc()