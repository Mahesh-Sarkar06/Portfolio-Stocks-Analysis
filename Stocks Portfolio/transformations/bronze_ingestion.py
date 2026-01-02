from pyspark import pipelines as dp
import dlt


# Incremental ingestion of daily data
@dp.table
def raw_daily_data():
    return (
        spark.readStream.format('cloudFiles')\
            .option('cloudFiles.format', 'json')\
            .option('cloudFiles.inferColumnTypes', 'true')\
            .option('multiLine', 'true')\
            .option('cloudFiles.schemaLocation', '/Volumes/portfolio/bronze/schema_location/')\
            .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')\
            .load('/Volumes/portfolio/bronze/raw_data')
    )