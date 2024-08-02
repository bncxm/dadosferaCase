#pylint: disable = missing-module-docstring, missing-function-docstring, logging-fstring-interpolation
import os
import logging
from datetime import datetime
import xml.etree.ElementTree as et
from io import BytesIO
import pandas as pd
import requests
from dotenv import load_dotenv
from requests_aws4auth import AWS4Auth
import s3fs


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
load_dotenv()

URI = os.getenv('URI_CLOUD')
ACCESS_KEY = os.getenv('ACCESS_CLOUD_KEY')
SECRET_KEY = os.getenv('SECRET_CLOUD_KEY')
HOST = os.getenv('HOST')
REGION = 'us-east-1'
BUCKET = 'raw'


def parse_xml(raw_bytes):
    try:
        root = et.fromstring(raw_bytes)
        namespace = {'ns': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        contents = root.findall('ns:Contents', namespace)


        latest_file = max(contents, key=lambda x: x.find('ns:LastModified', namespace).text)
        latest_key = latest_file.find('ns:Key', namespace).text

        return latest_key

    except et.ParseError as err:
        logging.error(f'Parse XML error: {err}')
        raise


def get_raw_bytes():
    try:
        awsauth = AWS4Auth(ACCESS_KEY, SECRET_KEY, REGION, 's3', service_type='s3')

        response = requests.get(
            f'{URI}/{BUCKET}',
            auth=awsauth,
            headers={'Host': HOST},
            timeout=30).content.decode('utf-8')

        return response

    except TypeError as err:
        logging.error(f'TypeError: {err}')
        raise
    except requests.exceptions.HTTPError as error:
        logging.error(f'HTTPError: {error}')
        raise


def load_file(latest_key):
    try:
        awsauth = AWS4Auth(ACCESS_KEY, SECRET_KEY, REGION, 's3', service_type='s3')
        response = requests.get(
            f'{URI}/{BUCKET}/{latest_key}',
            auth=awsauth,
            headers={'Host': HOST},
            timeout=30).content

        df = pd.read_parquet(BytesIO(response))
        return df

    except requests.exceptions.RequestException as err:
        logging.error(f'Request error: {err}')
        raise


def save_in_bucket(df):
    try:
        client = s3fs.S3FileSystem(
            anon= False,
            key=ACCESS_KEY,
            secret=SECRET_KEY,
            client_kwargs={'endpoint_url': URI}
        )

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f'reviews_{timestamp}.parquet'

        file_path = f'tmp/{file_name}'
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        df.to_parquet(file_path, index=False, engine='pyarrow')
        client.put(file_path, f'stage/{file_name}')

        logging.info('Success')

    except ValueError as error:
        logging.error("ValueError: %s", error)
    except FileNotFoundError as fnf_error:
        logging.error("FileNotFoundError: %s", fnf_error)
    except OSError as os_error:
        logging.error("OSError: %s", os_error)


def main():

    raw_bytes = get_raw_bytes()
    latest_key = parse_xml(raw_bytes)
    df = load_file(latest_key)


    df = df.drop_duplicates(subset=[
        'ProductId',
        'UserId',
        'ProfileName',
        'HelpfulnessNumerator',
        'HelpfulnessDenominator',
        'Score',
        'Time',
        'Summary',
        'Text'
        ])

    df = df.dropna()

    save_in_bucket(df)
