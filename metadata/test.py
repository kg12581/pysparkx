import pymysql
from pyspark.sql.functions import upper


def get_mysql_tables():
    try:
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='Admin@123456',
            database='qianfeng',
            cursorclass=pymysql.cursors.DictCursor,
            charset='utf8mb4'
        )
        cursor = connection.cursor()

        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s"
        database_name = 'qianfeng'
        cursor.execute(sql, (database_name,))

        tables = []
        for row in cursor.fetchall():
            print(row)
            # print('TABLE_NAME'))
            if 'TABLE_NAME' in row:
                tables.append(row['TABLE_NAME'])
            else:
                print('Warning: row does not contain table_name key')

        cursor.close()
        connection.close()
        return tables
    except pymysql.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return []


if __name__ == "__main__":
    tables = get_mysql_tables()
    print(tables)
