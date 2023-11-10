from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder.appName(
        "PySpark Data Processing Assignment with Trade Data"
    ).getOrCreate()

    # Load the dataset
    file_path = "effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Print the schema of the DataFrame
    df.printSchema()

    # Show the first few rows of the DataFrame
    df.show(5)

    # Simple data transformation - Selecting and filtering
    # Select relevant columns and filter based on a certain condition, for example, filtering for Exports only
    transformed_df = df.select("Year", "Date", "Country", "Value").filter(
        col("Direction") == "Exports"
    )
    transformed_df.show(5)

    # Spark SQL Query
    # Create a temporary view to run SQL queries
    df.createOrReplaceTempView("trade_data")

    # Example SQL query
    # Querying for total export value by year
    query_result = spark.sql(
        """
        SELECT Year, SUM(Value) as Total_Export_Value
        FROM trade_data 
        WHERE Direction = 'Exports'
        GROUP BY Year
        ORDER BY Year
        """
    )
    query_result.show()

    # Close the Spark session
    spark.stop()
