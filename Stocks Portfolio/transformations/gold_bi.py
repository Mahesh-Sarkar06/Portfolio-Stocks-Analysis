from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# Total Stock Count
@dp.materialized_view(
    name = 'portfolio.gold.stock_count',
    comment = 'View representing total stock in portfolio'
)
def stock_count():
    dim = spark.read.table('portfolio.silver.dim_stocks')
    fact = spark.read.table('portfolio.silver.fact_stocks')

    # Get latest record
    latest_record = fact.select(max('record_date').alias('curr_date'))

    # Joining to get latest record
    df_join = fact.join(latest_record, fact.record_date == latest_record.curr_date, 'inner')
    df_join = df_join.join(dim, on='stock_symbol', how='left')

    return (
        df_join.select('stock_name', 'stock_symbol', 'buy_date').dropDuplicates(['stock_name'])
    )


# 1-Day return of the stocks
@dp.materialized_view(
    name = 'portfolio.gold.daily_returns',
    comment = 'View that will represent the daily returns of the stocks'
)
def daily_returns():
    dim = spark.read.table('portfolio.silver.dim_stocks')
    fact = spark.read.table('portfolio.silver.fact_stocks')

    # Joining Dimension and Fact tables
    df_join = fact.join(dim, on='stock_symbol', how='inner')

    # Get all the records only when stock is bought
    df_join = df_join.where(col('buy_date') <= col('record_date'))

    # Creating Window by stocks to get previous closing price
    winSpec = Window.partitionBy('stock_symbol').orderBy('record_date')
    df_join = df_join.withColumn('prev_close', lag('close').over(winSpec))

    # Creating window to get latest record
    win_latest = Window.partitionBy('stock_symbol').orderBy(col('record_date').desc())
    df_join = df_join.withColumn('latest_record', row_number().over(win_latest))\
        .filter(col('latest_record') == 1).drop('latest_record')

    # Calculating the one day return
    df_join = df_join.withColumn('one_day_return', round(((col('close') - col('prev_close')) * 100) / col('prev_close'), 4))\
        .withColumn('year', year(col('record_date')))\
        .withColumn('month', month(col('record_date')))

    return (
        df_join.select('stock_symbol', 'record_date', 'close', 'prev_close', 'one_day_return', 'year', 'month')
    )


# Overall return of the stocks
@dp.materialized_view(
    name = 'portfolio.gold.overall_returns',
    comment = 'View that represent over return by stocks'
)
def overall_returns():
    dim = spark.read.table('portfolio.silver.dim_stocks')
    fact = spark.read.table('portfolio.silver.fact_stocks')

    # Joining both the table and filtering the records if stocks is bought
    df_join = fact.join(dim, on='stock_symbol', how='inner')
    df_join = df_join.filter(col('buy_date') <= col('record_date'))

    # Calculating the investment done
    winSpec = Window.partitionBy('stock_symbol').orderBy(col('record_date').desc())
    df_latest = df_join.withColumn('rnk', row_number().over(winSpec)).filter(col('rnk') == 1).drop('rnk')
    df_latest = df_latest.withColumn('initial_investment', col('avg_buy_price') * col('quantity'))\
        .withColumn('current_value', col('close') * col('quantity'))\
        .withColumn('profits', col('current_value') - col('initial_investment'))

    df_agg = df_latest.agg(round((sum('profits') * 100) / sum('initial_investment'), 4).alias('return_percent'))

    return df_agg.select('return_percent')


# Invested Amount
# @dp.materialized_view(
#     name = 'portfolio.gold.invested_amount',
#     comment = 'View for calculating the total amount invested'
# )
# def invested_amount():
#     dim = spark.read.table('portfolio.silver.dim_stocks')

#     df_total = dim.withColumn('total_investment', round(col('avg_buy_price') * col('quantity'), 2))

#     return (
#         df_total.select('stock_symbol', 'total_investment')
#     )



# Total P&L of the portfolio
@dp.materialized_view(
    name = 'portfolio.gold.total_pl',
    comment = 'View for representing the total P&L of the portfolio'
)
def total_pl():
    dim = spark.read.table('portfolio.silver.dim_stocks')
    fact = spark.read.table('portfolio.silver.fact_stocks')

    # Joining both the tables
    df_join = fact.join(dim, on='stock_symbol', how='inner')
    df_join = df_join.filter(col('record_date') >= col('buy_date'))

    # Getting the latest record for each stock
    winSpec = Window.partitionBy('stock_symbol').orderBy(col('record_date').desc())
    df_latest = df_join.withColumn('rnk', row_number().over(winSpec)).where(col('rnk') == 1).drop('rnk')

    # Calculating the every PnL
    df_latest = df_latest.withColumn('initial_value', col('avg_buy_price') * col('quantity'))\
        .withColumn('current_value', col('close') * col('quantity'))\
        .withColumn('PnL', col('current_value') - col('initial_value'))

    return (
        df_latest.agg(sum('initial_value').alias('total_invested'),
                      sum('current_value').alias('total_valuation'),
                      sum('PnL').alias('total_PnL'))\
                .select('total_invested', 'total_valuation', 'total_PnL')
    )



# Long term trends
@dp.materialized_view(
    name = 'portfolio.gold.dma200',
    comment = 'View for representing the 200 daily moving average'
)
def dma200():
    dim = spark.read.table('portfolio.silver.dim_stocks')
    fact = spark.read.table('portfolio.silver.fact_stocks')

    # Calculating only for the stocks available in the portfolio
    df_join = fact.join(dim, 'stock_symbol', 'inner')

    win_dma = Window.partitionBy('stock_symbol').orderBy(col('record_date').desc()).rowsBetween(-199, 0)
    #win_row = Window.partitionBy('stock_symbol').orderBy(col('record_date').desc())

    df_join = df_join.withColumn('DMA200', round(avg('close').over(win_dma), 4))\
            .withColumn('year', year(col('record_date')))\
            .withColumn('month', month(col('record_date')))

    return (
        df_join.select('stock_symbol', 'DMA200', 'record_date', 'year', 'month')
    )


# Total dividend received per stock
@dp.materialized_view(
    name = 'portfolio.gold.total_dividend',
    comment = 'View representing total dividends per stock'
)
def total_dividend():
    dim = spark.read.table('portfolio.silver.dim_stocks')
    div = spark.read.table('portfolio.silver.dim_dividends')

    df_join = div.join(dim, on='stock_name', how='left')
    df_join = df_join.where(col('record_date') >= col('buy_date'))

    df_join = df_join.withColumn('cumm_dividend', col('dividends') * col('quantity'))

    winSpec = Window.partitionBy('stock_name').orderBy('record_date')
    df_join = df_join.withColumn('total_dividend', sum('cumm_dividend').over(winSpec))

    return (
        df_join.select('stock_name', 'stock_symbol', 'total_dividend', 'record_date', year(col('record_date')).alias('year'), month(col('record_date')).alias('month'))
    )
