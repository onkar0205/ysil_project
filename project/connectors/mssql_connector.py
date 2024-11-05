from pyspark.sql import SparkSession

class MSSQLConnector:
    def __init__(self, jdbc_driver_path, host, port, database, user, password):
        self.jdbc_url = f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true"
        self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        self.user = user
        self.password = password
        self.spark = SparkSession.builder \
            .appName("MSSQLConnection") \
            .config("spark.jars", jdbc_driver_path) \
            .getOrCreate()

    def read_table(self, query):
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("query", query) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", self.driver) \
            .load()
