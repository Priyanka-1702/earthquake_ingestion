from google.cloud import storage
import os
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, split
from pyspark.sql import  SparkSession

spark = SparkSession.builder.appName("flatten_data").getOrCreate()
if __name__=='__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Lenovo N24\PycharmProjects\cloud_project\prj-project1-7ff7f312bdc3.json'

    def download_json_from_gcs(bucket_name, file_path):
        """
        Download JSON file as string from gcs.
        :param bucket_nae:
        :param file_path:
        :return:
        """
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        json_string = blob.download_as_string()
        return json_string

    def convert_timestamp_to_gmt(timestamp_ms):
        if timestamp_ms is not None:
            timestamp_ms = timestamp_ms / 1000
            return datetime.utcfromtimestamp(timestamp_ms).strftime('%Y-%m-%d %H:%M%S')
        return None


    def flatten_data_convert_to_df(json_string):
        data = json.loads(json_string)

        flattened_data = []

        for feature in data['features']:
            properties = feature['properties']
            geometry = feature['geometry']
            coordinates = geometry['coordinates']

            flattened_record = {
                'mag': float(properties.get('mag')) if properties.get('mag') is not None else None,
                'place':properties.get('place'),
                'time':convert_timestamp_to_gmt(properties.get('time')),
                'updated': convert_timestamp_to_gmt(properties.get('updated')),
                'tz': properties.get('tz'),
                'url': properties.get('url'),
                'detail': properties.get('detail'),
                'felt': properties.get('felt'),
                'cdi': float(properties.get('cdi')) if properties.get('cdi') is not None else None,
                'mmi': float(properties.get('mmi')) if properties.get('mmi') is not None else None,
                'alert': properties.get('alert'),
                'status': properties.get('status'),
                'tsunami': properties.get('tsunami'),
                'sig': properties.get('sig'),
                'net': properties.get('net'),
                'code': properties.get('code'),
                'ids': properties.get('ids'),
                'sources': properties.get('sources'),
                'types': properties.get('types'),
                'nst': properties.get('nst'),
                'dmin': float(properties.get('dmin')) if properties.get('dmin') is not None else None,
                'rms':float(properties.get('rms'))if properties.get('rms') is not None else None,
                'gap': float(properties.get('gap')) if properties.get('gap') is not None else None,
                'magType': properties.get('magType'),
                'type': properties.get('type'),
                'title': properties.get('title'),
                'geometry': {
                    'longitude':coordinates[0],  # Corrected spelling
                    'latitude': coordinates[1],
                    'depth': float(coordinates[2]) if coordinates[2] is not None else None
                }
            }

            flattened_data.append(flattened_record)
        # Define schema
        schema = StructType([
            StructField("mag", FloatType(), True),
            StructField("place", StringType(), True),
            StructField("time", StringType(), True),
            StructField("updated", StringType(), True),
            StructField("tz", StringType(), True),
            StructField("url", StringType(), True),
            StructField("detail", StringType(), True),
            StructField("felt", StringType(), True),
            StructField("cdi", StringType(), True),
            StructField("mmi", StringType(), True),
            StructField("alert", StringType(), True),
            StructField("status", StringType(), True),
            StructField("tsunami", IntegerType(), True),
            StructField("sig", IntegerType(), True),
            StructField("net", StringType(), True),
            StructField("code", StringType(), True),
            StructField("ids", StringType(), True),
            StructField("sources", StringType(), True),
            StructField("types", StringType(), True),
            StructField("nst", IntegerType(), True),
            StructField("dmin", FloatType(), True),
            StructField("rms", FloatType(), True),
            StructField("gap", FloatType(), True),
            StructField("magType", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("longitude", FloatType(), True),
            StructField("latitude", FloatType(), True),
            StructField("depth", FloatType(), True)
        ])

        return spark.createDataFrame(flattened_data, schema=schema)


    bucket_name = "earthquake_analysis_1"
    # current_date = datetime.now()
    # formatted_date = current_date.strftime('%Y%m%d')
    file_path = "pyspark/landing/20241021/earthquake_raw.json"
    json_string = download_json_from_gcs(bucket_name,file_path)
    df = flatten_data_convert_to_df(json_string)

    df.show()

    def to_add_column(df):
        df1 = df.withColumn("area", split(col('place'), 'of').getItem(1))
        return df1

    df1 = to_add_column(df)
    current_date = datetime.now()
    formatted_date = current_date.strftime('%Y%m%d')
    gcs_path = f"gs://earthquake_analysis_1/pyspark/silver/{formatted_date}/earthquake_flatten.json"

    df1.write.json(gcs_path, mode='overwrite')
