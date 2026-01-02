from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Transforming the ingested data
@dp.table(
    name = 'portfolio.silver.flatten_data',
    comment = 'Flattening JSON data'
)
def flatten_data():
    df = spark.readStream.table('portfolio.bronze.raw_daily_data')

    # Fetching column names from nested JSON columns
    df_flatten = df.withColumn('avg_buy_price', col('records.avg_buy_price'))\
        .withColumn('buy_date', col('records.buy_date'))\
        .withColumn('quantity', col('records.quantity'))\
        .withColumn('stock_status', col('records.stock_status'))\
        .withColumn('open', col('daily_records.open_price'))\
        .withColumn('high', col('daily_records.high_price'))\
        .withColumn('low', col('daily_records.low_price'))\
        .withColumn('close', col('daily_records.close_price'))\
        .withColumn('volume', col('daily_records.volume'))\
        .withColumn('record_date', col('daily_records.record_date'))\
        .withColumn('dividends', col('events.dividends'))\
        .withColumn('stock_splits', col('events.stock_splits'))\
        .withColumn('adjustment_factor', col('events.adjustment_factor'))

    return (
        df_flatten.select('stock_name', 'stock_symbol', 'avg_buy_price', 'buy_date', 'quantity', 'stock_status', 'open', 'high', 'low', 'close', 'volume', 'record_date', 'dividends', 'stock_splits', 'adjustment_factor')
    )



# Staging Layer for normalizing the dimension table data
@dp.materialized_view(
    name = 'portfolio.silver.dim_stocks',
    comment = 'Materialized View of unique stocks'
)
def dim_stocks():
    df_stocks = spark.read.table('portfolio.silver.flatten_data')

    # Creating a window to get last buying record for each stock
    winSpec = Window.partitionBy('stock_name').orderBy(col('record_date').desc())
    df_stocks = df_stocks.withColumn('rn', row_number().over(winSpec)).where(col('rn') == 1).drop('rn')

    df_stocks = df_stocks.filter(col('stock_status') == 'BUY')

    # Calculating the actual stocks after splitting
    df_stocks = df_stocks.withColumn('quantity', col('quantity') * when(col('adjustment_factor') > 0.0, col('adjustment_factor')).otherwise(1))

    # Calculating the new avg buy price
    df_stocks = df_stocks.withColumn('new_price', round(col('avg_buy_price') / when(col('adjustment_factor') > 0.0, col('adjustment_factor')).otherwise(1), 2))

    # Adjusting the fractional share
    df_stocks = df_stocks.withColumn('adj_qty', floor(col('quantity')))
    # Fractional part
    df_stocks = df_stocks.withColumn('frac_qty', (col('quantity') - col('adj_qty')))

    # Cash back
    df_stocks = df_stocks.withColumn('cash_back', (col('frac_qty') * col('new_price')))

    # Adjusting the avg buy price
    df_final = df_stocks.withColumn('avg_buy_price', when(col('adjustment_factor') > 0.0, col('new_price')).otherwise(col('avg_buy_price')))

    return (
        df_final.select('stock_name', 'stock_symbol', 'avg_buy_price', col('adj_qty').alias('quantity'), 'buy_date')
    )



# Staging layer for normalizing the dividend table data
@dp.materialized_view(
    name = 'portfolio.silver.dim_dividends',
    comment = 'Materialized View for dividends data'
)

def dim_dividends():
    df_div = spark.read.table('portfolio.silver.flatten_data')
    df_div = df_div.filter(col('stock_status') == 'BUY')
    
    return (
        df_div.select('stock_name', 'dividends', 'record_date', year('record_date').alias('year')).where(col('dividends') > 0.0)
    )



# Normalizing the fact table
@dp.materialized_view(
    name = 'portfolio.silver.fact_stocks',
    comment = 'Materialized View for historical data of the stocks'
)
def fact_stocks():
    df_stocks = spark.read.table('portfolio.silver.flatten_data')
    df_stocks = df_stocks.filter(col('stock_status') == 'BUY')

    # Creating a window by stock names and reading all previous records from current row
    winSpec = Window.partitionBy('stock_name').orderBy('record_date').rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Cummulative factor for calculating adjusting factor
    df_stocks = df_stocks.withColumn('cumm_fact', exp(sum(log(when(col('adjustment_factor') > 0.0, col('adjustment_factor')).otherwise(1))).over(winSpec)))

    # Adjusting the column
    df_stocks = df_stocks.withColumn('open', round(col('open') / col('cumm_fact'), 2))\
        .withColumn('high', round(col('high') / col('cumm_fact'), 2))\
        .withColumn('low', round(col('low') / col('cumm_fact'), 2))\
        .withColumn('close', round(col('close') / col('cumm_fact'), 2))\
        .withColumn('volume', round(col('volume') * col('cumm_fact'), 2))

    return (
        df_stocks.select('stock_symbol', 'open', 'high', 'low', 'close', 'volume', 'record_date')
    )