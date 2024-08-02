#pylint: disable = missing-module-docstring, missing-function-docstring, logging-fstring-interpolation
import os
import logging
import xml.etree.ElementTree as et
import requests
from dotenv import load_dotenv
from requests_aws4auth import AWS4Auth


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
load_dotenv()

URI = os.getenv('URI')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
HOST = os.getenv('HOST')
REGION = 'us-east-1'
BUCKET = 'curated'


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


def get_curated_bytes():
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

        # df = pd.read_parquet(BytesIO(response))
        return response

    except requests.exceptions.RequestException as err:
        logging.error(f'Request error: {err}')
        raise


def main():

    raw_bytes = get_curated_bytes()
    latest_key = parse_xml(raw_bytes)
    data = load_file(latest_key)

    with open('latest_file.parquet', 'wb') as file_data:
        file_data.write(data)
