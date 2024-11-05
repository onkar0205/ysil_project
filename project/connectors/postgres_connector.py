from pyspark.sql import SparkSession

class PostgresConnector:
    def __init__(self, jdbc_driver_path, host, port, database, user, password):
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        self.driver = "org.postgresql.Driver"
        self.user = user
        self.password = password
        self.spark = SparkSession.builder \
            .appName("PostgresConnection") \
            .config("spark.jars", jdbc_driver_path) \
            .getOrCreate()
        self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


    def read_table(self, query):
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("query", query) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", self.driver) \
            .load()
