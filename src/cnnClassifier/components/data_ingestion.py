import os
import zipfile
import gdown
from cnnClassifier import logger
from cnnClassifier.utils.common import get_size
import boto3
from src.cnnClassifier.entity.config_entity import DataIngestionConfig

def download_file_from_s3(bucket_name, s3_file_key, local_file_path):
    s3 = boto3.client('s3')

    try:
        # Download the file from S3
        s3.download_file(bucket_name, s3_file_key, local_file_path)
        print(f"File {s3_file_key} downloaded from bucket {bucket_name} to {local_file_path}.")
    except Exception as e:
        print(f"Error downloading file from S3: {e}")

class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    
    def download_file(self)-> str:
        '''
        Fetch data from the url
        '''

        try: 
            dataset_url = self.config.source_URL
            zip_download_dir = self.config.local_data_file
            os.makedirs("artifacts/data_ingestion", exist_ok=True)
            logger.info(f"Downloading data from {dataset_url} into file {zip_download_dir}")

            if dataset_url.startswith("s3://"):
                dataset_url = dataset_url[5:]
    
            # Split the URL into bucket name and file key
            bucket_name, s3_file_key = dataset_url.split('/', 1)
            print(bucket_name)
            print(s3_file_key)
            print("==============")
            # prefix = 'https://drive.google.com/uc?/export=download&id='
            download_file_from_s3(bucket_name, s3_file_key, zip_download_dir)
            #local_file_path = os.path.join(os.getcwd(), 'kidney-data-scan-images.zip')
            #gdown.download(prefix+file_id,zip_download_dir)

            logger.info(f"Downloaded data from {dataset_url} into file {zip_download_dir}")

        except Exception as e:
            raise e
        
    

    def extract_zip_file(self):
        """
        zip_file_path: str
        Extracts the zip file into the data directory
        Function returns None
        """
        unzip_path = self.config.unzip_dir
        os.makedirs(unzip_path, exist_ok=True)
        with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
            zip_ref.extractall(unzip_path)