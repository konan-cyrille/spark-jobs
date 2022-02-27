import convert_csv_files_to_parquet_file

class TestConvertCsvToParquet:
    def test_set_sparkSession(self):
        spark = convert_csv_files_to_parquet_file.set_SparkSession("Ma section spark")
        assert spark == spark.getActiveSession()