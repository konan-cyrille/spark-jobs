import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, lit


def set_SparkSession(app_name) -> object:
    spark = (
        SparkSession
        .builder
        .appName(app_name)
        .getOrCreate()
    )
    # Si on veut utiliser le format legal sur les dates
    # il est important d'ajouter cette config dans spark Config
    # permet de mettre le mode overwrite de partitions
    # des fichier en mode dynamic
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def spark_reader(spark_object, path, file_format) -> object:
    data = (
        spark_object
        .read
        .format(file_format)
        .option('inferSchema', 'true')
        .option('header', 'true')
        .load(path)
    )
    return data


if __name__ == '__main__':
    folder_raw = "/home/konan/Documents/big-data/spark/data/stock_market/raw/"
    folder_business = "/home/konan/Documents/big-data/spark/data/stock_market/business/"
    # recupération des path de tout les fichiers csv du dossier stock_market
    # list_of_csv_file_path = [comp_code for market_dir in glob.glob(folder + '*') for csv_json_dir in glob.glob(market_dir + '/csv') for comp_code in glob.glob(csv_json_dir + '/*')]
    list_of_csv_file_path = [comp_code for market_dir in glob.glob(folder_raw + '*') for csv_json_dir in glob.glob(market_dir + '/*') for comp_code in glob.glob(csv_json_dir + '/*.csv')]
    # permet de verifier que touts les elements de list_of_csv_file_path sont des fichiers csv
    [lf for lf in list_of_csv_file_path if not lf.endswith('.csv')]
    for path_to_read in list_of_csv_file_path:
        print("----------------Start--------------------")
        market, _, comp_code = path_to_read.split('/')[-3:]
        comp_code = comp_code.split('.')[0]
        print(market)
        print(comp_code)
        spark = set_SparkSession("Convert a couple of csv file into parquet file")
        data = spark_reader(spark_object=spark, path=path_to_read, file_format='csv')
        data.printSchema()
        print(data.rdd.getNumPartitions())
        print("Making some transformations")
        data = (
            data
            .withColumn('market', lit(market))
            .withColumn('company_code', lit(comp_code))
        )
        data.printSchema()
        data.show(5)
        data = (
            data
            .withColumnRenamed('Adjusted Close', 'Adjusted_Close')
            .withColumn('Days', to_date(data.Date, "dd-MM-yyyy"))
            .select(['Days', 'market', 'company_code', 'Low', 'Open', 'Volume', 'High', 'Close', 'Adjusted_Close'])
        )
        data.printSchema()
        print("writing to parquet file partitioning by market and company code")
    #     folder_business = f"/home/konan/Documents/big-data/spark/data/stock_market/business/{market}/{comp_code}"
        (
            data
            .write
            .format('parquet')
            .mode('overwrite')
            .partitionBy(['market', 'company_code'])
            .save(folder_business)
        )
        print("----------------End--------------------")
