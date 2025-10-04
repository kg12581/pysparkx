# 暂时不要这个了 要配置java home环境





import jaydebeapi
import jpype


def read_postgresql_data():
    import yaml

    with open("/Users/kgt/code/test/pysparkx/conf/settings.yaml", "r", encoding="utf-8") as f:
        # 安全加载（推荐，避免执行恶意代码）
        config = yaml.safe_load(f)

    print(config["postgresql"]["user_name"])  # 输出：localhost

    # 1. 配置连接参数
    driver_class = "org.postgresql.Driver"  # PostgreSQL JDBC 驱动类名
    jdbc_url = "jdbc:postgresql://localhost:5432/qianfeng03"  # 数据库连接URL
    # 格式：jdbc:postgresql://主机地址:端口/数据库名（默认端口5432）
    username = config["postgresql"]["user_name"]  # 数据库用户名
    password = config["postgresql"]["password"]  # 数据库密码
    jar_path = "/Users/kgt/code/test/pysparkx/lib/postgresql-42.7.6.jar"  # 下载的JDBC驱动路径

    # 2. 启动JVM（jpype1需要手动启动，避免重复启动报错）
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), f"-Djava.class.path={jar_path}")

    # 3. 建立数据库连接
    conn = None
    try:
        conn = jaydebeapi.connect(
            driver_class,
            jdbc_url,
            [username, password],
            jar_path
        )
        print("PostgreSQL 连接成功！")

        # 4. 创建游标并执行查询
        with conn.cursor() as cursor:
            # 示例：查询表中数据（替换为你的表名和SQL）
            sql = "SELECT id, name, age FROM users WHERE age > 18 LIMIT 5"
            cursor.execute(sql)

            # 5. 获取查询结果
            columns = [column[0] for column in cursor.description]  # 获取列名
            print("\n查询结果列名：", columns)

            results = cursor.fetchall()  # 获取所有行数据
            print("查询结果数据：")
            for row in results:
                # 可将行数据与列名组合为字典（更易读）
                row_dict = dict(zip(columns, row))
                print(row_dict)

    except Exception as e:
        print(f"操作失败：{str(e)}")
    finally:
        # 6. 关闭连接和JVM（按需关闭，多次操作可保留JVM）
        if conn:
            conn.close()
            print("\n数据库连接已关闭")
        # 若后续不再使用，可关闭JVM（注意：关闭后需重新启动才能再次连接）
        # if jpype.isJVMStarted():
        #     jpype.shutdownJVM()


if __name__ == "__main__":
    read_postgresql_data()
