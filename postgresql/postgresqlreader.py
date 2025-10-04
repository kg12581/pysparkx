from pyspark.sql import SparkSession


def postgresql_read():
    # 初始化SparkSession
    spark = SparkSession.builder \
        .appName("Read from PostgreSQL") \
        .config("spark.driver.extraClassPath", "/Users/kgt/code/test/pysparkx/lib/postgresql-42.7.6.jar") \
        .getOrCreate()

    # PostgreSQL连接参数
    db_url = "jdbc:postgresql://localhost:5432/information_schema"
    table_name = "answer_paper"
    properties = {
        "user": "admin",
        "password": "Admin@123456",
        "driver": "org.postgresql.Driver"
    }

    # 方法1: 读取整个表
    df = spark.read.jdbc(
        url=db_url,
        table=table_name,
        properties=properties
    )

    # 方法2: 通过SQL查询读取部分数据
    query = "(SELECT * FROM answer_paper) AS tmp".format(table_name)
    df = spark.read.jdbc(
        url=db_url,
        table=query,
        properties=properties
    )

    # 显示数据
    df.show()

    # 查看数据结构
    df.printSchema()

    # 停止SparkSession
    spark.stop()


if __name__ == '__main__':
    postgresql_read()