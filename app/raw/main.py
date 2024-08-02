#pylint: disable = missing-module-docstring, missing-function-docstring
import os
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import s3fs


load_dotenv()
# suppressing libs logs
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# creating specific INFO log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

URI = os.getenv('URI')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
CSV_PATH = os.getenv('CSV_PATH')


def read_csv():
    df = pd.read_csv(CSV_PATH)
    return df

def main():
    try:
        client = s3fs.S3FileSystem(
            anon= False,
            key=ACCESS_KEY,
            secret=SECRET_KEY,
            client_kwargs={'endpoint_url': URI}
        )
        df = read_csv()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f'reviews_{timestamp}.parquet'

        file_path = f'tmp/{file_name}'
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        df.to_parquet(file_path, index=False, engine='pyarrow')
        client.put(file_path, f'raw/{file_name}')

        logger.info('Success')


    except ValueError as error:
        logger.error("ValueError: %s", error)
    except FileNotFoundError as fnf_error:
        logger.error("FileNotFoundError: %s", fnf_error)
    except OSError as os_error:
        logger.error("OSError: %s", os_error)
