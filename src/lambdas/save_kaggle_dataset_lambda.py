from ast import main
import sys
import logging
from kaggle import KaggleApi


logger = logging.getLogger("Lambda")
logging.basicConfig()
logs_level = "INFO"
logger.setLevel(logs_level)
logger.info(f"Logger initialized. Set level to: {logs_level}")


def find_dataset(kaggle_api,dataset_name):
    kaggle_dataset = kaggle_api.dataset_list(search=dataset_name)
    if len(kaggle_dataset) > 1:
        logger.info(f"Found {len(kaggle_dataset)} datasets:")
        for dataset in kaggle_dataset:
            logger.info(dataset)
        logger.error("ERROR: This API can process only one dataset at a time!")  # TODO: return this 
        logger.error("Please input the exact name of the dataset like this: username/dataset-name.")
        return 1
    else:
        logger.info(f"Found dataset {kaggle_dataset}!")
        return 0
    

def load_dataset_to_s3():
    pass


def load_dataset_metadata_to_s3():
    pass


def lambda_handler(event, context):
    kaggle_api_obj = KaggleApi()
    kaggle_api_obj.authenticate()

    dataset_to_retrieve = event['dataset_name']
    return find_dataset(kaggle_api_obj, dataset_to_retrieve)
     

def main():
    event = {'dataset_name': 'martj42/international-football-results-from-1872-to-2017'}
    context = ''
    print(f'Returned: {lambda_handler(event, context)}')
    

if __name__ == '__main__':
    main()

# algorithm:
# 1. check if dataset exists
# 2. get dataset metadata
# 3. check if dataset is structured (API will only handle structured datasets)
# 4. check if dataset is less than 80MB (for now)
# 5. check if event has S3 location for data
# 6. if event doesn't have S3 location, check if env variable has S3 data location set
# 7. place dataset data and metadata on S3 (location either from env var or event, 
# event location always in priority)
# 8. make sure that data and metadata are on S3
# 9. generate athena query using table metadata
# 10. run athena query to create a glue table
# 11. check if glue table was properly created (all column are present, all data types
# are correct, amount of rows is correct)
