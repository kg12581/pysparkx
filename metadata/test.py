import pymysql


def get_hive_metadata():
    # 连接MySQL数据库，这里假设Hive元数据存储在MySQL中
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='Admin@123456',
        database='hive_metastore',
        charset='utf8mb4'
    )

    try:
        with connection.cursor() as cursor:
            # 查询DBS表获取所有数据库信息
            sql = "SELECT * FROM DBS"
            cursor.execute(sql)
            databases = cursor.fetchall()
            for db in databases:
                print(f"Database: {db}")

            # 查询TBLS表获取所有表信息
            sql = "SELECT * FROM TBLS"
            cursor.execute(sql)
            tables = cursor.fetchall()
            for table in tables:
                print(f"Table: {table}")

            # 查询COLUMNS_V2表获取所有列信息
            sql = "SELECT * FROM COLUMNS_V2"
            cursor.execute(sql)
            columns = cursor.fetchall()
            for column in columns:
                print(f"Column: {column}")

    finally:
        connection.close()


if __name__ == "__main__":
    get_hive_metadata()