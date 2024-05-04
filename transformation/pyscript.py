import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def create_spark_session(name):
    return (SparkSession
                .builder
                .master('yarn')
                .appName(name)
                .getOrCreate()
    )


def get_file_path(file_paths_str):
    file_paths = file_paths_str.split('s3:')
    file_paths = [path.strip() for path in file_paths if path.strip()]
    s3_file_paths = ['s3:' + path for path in file_paths]
    return s3_file_paths

def create_df(spark,filepath):
     return spark.read.csv(filepath, inferSchema=True, header=True, sep=',')
  

def join_dataframe(sales_df, calendar_df, inventory_df):
    return (sales_df.alias('s') 
    .join(calendar_df.alias('c'),on=expr("s.TRANS_DT=c.CAL_DT"),how="inner") 
    .join(inventory_df.alias('i'),on=expr("s.STORE_KEY=i.STORE_KEY and s.PROD_KEY=i.PROD_KEY"),how="inner") 
    .select('c.YR_NUM',
        'c.WK_NUM',
        'c.DAY_OF_WK_NUM',
        'c.MNTH_NUM',
        's.STORE_KEY',
        's.PROD_KEY',
        's.SALES_QTY',
        's.SALES_AMT',
        's.SALES_COST',
        'i.INVENTORY_ON_HAND_QTY',
        'i.INVENTORY_ON_ORDER_QTY',
        'i.OUT_OF_STOCK_FLG'
))


def group_dataframe(joined_df):
    return (joined_df.groupBy('YR_NUM','WK_NUM','STORE_KEY','PROD_KEY'))


def get_low_stock(joined_df):
    return (joined_df.select(
    'YR_NUM',
    'WK_NUM',
    'STORE_KEY',
    'PROD_KEY',
    'INVENTORY_ON_HAND_QTY',
    'SALES_QTY')
 .withColumn('LOW_STOCK_FLG', when(col('INVENTORY_ON_HAND_QTY') < col('SALES_QTY'), 1).otherwise(0)          
 ))


def aggregate1(grouped_df):
    return (grouped_df.agg(
    sum('SALES_QTY').alias('TOTAL_SALES_QTY'),
    sum('SALES_AMT').alias('TOTAL_SALES_AMT'),
    round(sum('SALES_COST'),3).alias('TOTAL_COST_WEEK'),
    round((sum('SALES_AMT')/sum('SALES_QTY')),3).alias('AVG_SALES_PRICE'),
    round(((sum('OUT_OF_STOCK_FLG')/7)*100),3).alias('PERCENTAGE_STORE_IN_STOCK'),
    sum('OUT_OF_STOCK_FLG').alias('NO_STOCK_INSTANCE')
))


def aggregate2(joined_df, agg1):
    return (joined_df.filter("DAY_OF_WK_NUM=6")
 .select('YR_NUM',
         'MNTH_NUM',
         'WK_NUM',
         'DAY_OF_WK_NUM',
         'STORE_KEY',
         'PROD_KEY',
         'INVENTORY_ON_HAND_QTY',
         'INVENTORY_ON_ORDER_QTY',
         'OUT_OF_STOCK_FLG').alias('j')
 .join(agg1.alias('a'),
       on=expr("j.STORE_KEY=a.STORE_KEY and j.PROD_KEY=a.PROD_KEY and j.YR_NUM=a.YR_NUM and j.WK_NUM=a.WK_NUM"),
       how="inner")
 .select('a.*','j.MNTH_NUM','j.INVENTORY_ON_HAND_QTY','j.INVENTORY_ON_ORDER_QTY', 'j.OUT_OF_STOCK_FLG')
 )

 
def aggregate3(low_stock, agg2):
    return (low_stock.alias('l')
    .join(agg2.alias('a'),
          on=expr("l.STORE_KEY=a.STORE_KEY and l.PROD_KEY=a.PROD_KEY and l.YR_NUM=a.YR_NUM and l.WK_NUM=a.WK_NUM"),
          how="inner")
    .select('a.*', 'l.LOW_STOCK_FLG')
    )


def final_dataframe(agg3):
    desired_columns = ['YR_NUM',
         'MNTH_NUM',
         'WK_NUM',
         'STORE_KEY',
         'PROD_KEY',
         'TOTAL_SALES_QTY',
         'TOTAL_SALES_AMT',
         'TOTAL_COST_WEEK',
         'AVG_SALES_PRICE',
         'PERCENTAGE_STORE_IN_STOCK',
         'NO_STOCK_INSTANCE',
         'INVENTORY_ON_HAND_QTY',
         'INVENTORY_ON_ORDER_QTY',
         'OUT_OF_STOCK_FLG',
         'LOW_STOCK_FLG'
    ]
    return agg3.select(desired_columns)


def write_parquet(final_df, output_path, mode, compression_type):
    (final_df.write.mode(mode)
    .partitionBy('YR_NUM', 'WK_NUM')
    .option("compression", compression_type)
    .parquet(f"{output_path}/sales_inv/"))


def write_csv(dataframe, output_path, mode):
    (dataframe.repartition(1)
    .write.mode(mode)
    .option('header', True)
    .csv(output_path))


def main(): 

    DATE = sys.argv[1]
    file_paths_str = sys.argv[2]
    OUTPUT_BUCKET = "s3://retail_data_processed/"
    
    #create sparkSession
    spark = create_spark_session('batch_processing')
    
    #getting file paths
    s3_file_paths = get_file_path(file_paths_str)
    sales_path = s3_file_paths[0]
    calendar_path = s3_file_paths[1]
    inventory_path = s3_file_paths[2]
    store_path = s3_file_paths[3]
    product_path = s3_file_paths[4]

    #create dataFrame
    sales_df = create_df(spark, sales_path)
    inventory_df = create_df(spark, inventory_path) 
    calendar_df = create_df(spark, calendar_path) 
    store_df = create_df(spark, store_path)
    product_df = create_df(spark, product_path)

    #aggregating the dataframes
    joined_df = join_dataframe(sales_df, calendar_df, inventory_df)
    grouped_df = group_dataframe(joined_df)
    low_stock = get_low_stock(joined_df)
    agg1 = aggregate1(grouped_df)
    agg2 = aggregate2(joined_df, agg1)
    agg3 = aggregate3(low_stock, agg2)
    final_df = final_dataframe(agg3)

    #write the final dataframe into s3
    write_parquet(final_df, OUTPUT_BUCKET, 'overwrite', 'gzip')
    write_csv(store_df, f'{OUTPUT_BUCKET}/store/date={DATE}', 'overwrite')
    write_csv(product_df, f'{OUTPUT_BUCKET}/product/date={DATE}', 'overwrite')
    write_csv(calendar_df, f'{OUTPUT_BUCKET}/calendar/date={DATE}', 'overwrite')


if __name__ == "__main__":
    main()