from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("mysql_to_pg_pysparksql_etl") \
    .config("spark.jars","/Users/kgt/code/test/pysparkx/lib/mysql-connector-java-8.0.28.jar") \
    .getOrCreate()

# ===== MySQL =====
mysql_url = "jdbc:mysql://localhost:3306/qianfeng?useSSL=false"
mysql_props = {
    "user": "admin",
    "password": "Admin@123456",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df_mysql = spark.read.jdbc(
    url=mysql_url,
    table="question_difficulty",
    column="id",
    lowerBound=1,
    upperBound=10000000,
    numPartitions=8,
    properties=mysql_props
)

df_mysql.createOrReplaceTempView("ods_orders")

spark.sql("select count(1) from ods_orders").show()


# ===== ETL (Spark SQL) =====
'''
spark.sql("""
CREATE OR REPLACE TEMP VIEW dwd_orders AS
SELECT
  id,
  user_id,
  order_amount,
  order_status,
  order_time,
  to_date(order_time) AS order_date,
  CASE
    WHEN order_status = 'SUCCESS' THEN 1
    ELSE 0
  END AS is_success
FROM ods_orders
WHERE order_amount > 0
""")


df_result = spark.sql("""
SELECT
  order_date,
  COUNT(*) AS order_cnt,
  SUM(order_amount) AS total_amount,
  SUM(is_success) AS success_order_cnt
FROM dwd_orders
GROUP BY order_date
""")
'''


# ===== PostgreSQL =====
pg_url = "jdbc:postgresql://pg-host:5432/dw_db"
pg_props = {
    "user": "pg_user",
    "password": "pg_pwd",
    "driver": "org.postgresql.Driver",
    "batchsize": "5000",
    "rewriteBatchedInserts": "true"
}

# 写入pg
# df_result.write \
#     .mode("append") \
#     .jdbc(pg_url, "dws_order_day", properties=pg_props)

spark.stop()
