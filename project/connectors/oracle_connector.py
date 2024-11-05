from pyspark.sql import SparkSession

class OracleConnector:
    def __init__(self, jdbc_driver_path, host, port, database, user, password):
        self.jdbc_url = f"jdbc:oracle:thin:@{host}:{port}/{database}"
        self.driver = "oracle.jdbc.OracleDriver"
        self.user = user
        self.password = password
        self.spark = SparkSession.builder \
            .appName("OracleDBConnection") \
            .config("spark.jars", jdbc_driver_path) \
            .getOrCreate()
        
    def read_table(self, query):
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("query", query)\
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", self.driver) \
            .load()
