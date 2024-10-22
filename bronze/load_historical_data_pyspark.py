import os
import requests
import json

# from pyspark.sql import SparkSession
from google.cloud import storage
from datetime import date, datetime

if __name__=='__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Lenovo N24\PycharmProjects\cloud_project\prj-project1-7ff7f312bdc3.json'

    def fetch_data_from_api(api_url):
        """
        Fetch data from the given url and return the data
        :param api_url:  The url of the API to fetch data from.
        :return: The data fetched from API (JASON Format) or none if the request failed.
        """
        try:
            # Make the get request to the API
            response = requests.get(api_url)

            # check if the request was successful
            if response.status_code == 200:
                return response.json() # Parse and return json data
            else:
                print(f"failed to fetch data. Status code: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:

            print(f"An error occurred : {e}")
            return None

    def upload_to_gcs(bucket_name, destination_blob_name, data):
        """
        Uploads teh given data (JSON format) to google cloud storage
        :param bucket_name: Name of the GCS bucket
        :param destination_blob_name: The name of the destination file in GCS.
        :param data: JSON data to upload.
        """
        # Initialize GCS client
        client = storage.Client()

        # Get teh bucket
        bucket = client.bucket(bucket_name)

        # Create a blob (GCS object) in the bucket
        blob = bucket.blob(destination_blob_name)

        # Convert data to JSON string before uploading
        json_data = json.dumps(data)

        # upload data as a file
        blob.upload_from_string(json_data, content_type="application/json")

        print(f"File {destination_blob_name}uploaded to GCS bucket {bucket_name}.")

    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    # fetch data from API
    data = fetch_data_from_api(api_url)

    if data:
        # Define GCS bucket and file name
        bucket_name = "earthquake_analysis_1"
        current_date = datetime.now()
        formatted_date = current_date.strftime('%Y%m%d')
        destination_blob_name =f"pyspark/landing/{formatted_date}/earthquake_raw.json"

        # Upload the data to GCS
        upload_to_gcs(bucket_name, destination_blob_name, data)
    else:
        print("no data fetched to upload")





