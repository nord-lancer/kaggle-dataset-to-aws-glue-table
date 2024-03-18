from ast import main
from math import log
import sys
import logging
from kaggle import KaggleApi


KAGGLE_DATASET_NAME_SLASH = '/'


logger = logging.getLogger("Lambda")
logging.basicConfig()
logs_level = "DEBUG"
logger.setLevel(logs_level)
logger.info(f"Logger initialized. Set level to: {logs_level}")


def find_dataset(kaggle_api,dataset_name):
    kaggle_dataset = kaggle_api.dataset_list(search=dataset_name)
    
    if len(kaggle_dataset) > 1:
        logger.info(f"Found {len(kaggle_dataset)} datasets:")
        for dataset in kaggle_dataset:
            logger.info(dataset)
        logger.error("ERROR: This API can process only one dataset at a time!")  # TODO: return this message from the API
        logger.error("Please input the exact name of the dataset like this: "
                     "username/dataset-name.")
        return 1
    else:
        logger.info(f"Found dataset {kaggle_dataset}!")
        logger.debug(f"Dataset type: {type(kaggle_dataset)}")
        return kaggle_dataset[0]  # return a dataset object instead of a list with 1 object in it
    

def get_dataset_files_list(kaggle_api, dataset):
    # TODO: check for error message before returning the file list?
    dataset_full_name = (dataset.ref
                         + KAGGLE_DATASET_NAME_SLASH
                         + str(dataset.currentVersionNumber))
    kaggle_dataset_files_list = kaggle_api.dataset_list_files(dataset_full_name)
    if kaggle_dataset_files_list.error_message:
        logger.error(f"When retrieving kaggle dataset files list, received "
                     f"the following error message: "
                     f"{kaggle_dataset_files_list.error_message}")
    logger.info(f"Kaggle dataset files: {kaggle_dataset_files_list.files}")
    return kaggle_dataset_files_list.files


def get_dataset_metadata(kaggle_api,dataset_name):
     kaggle_dataset = kaggle_api.dataset_list(search=dataset_name)
     pass


def load_dataset_to_s3():
    pass


def load_dataset_metadata_to_s3():
    pass


def get_authenticated_kaggle_api_obj():
    kaggle_api_obj = KaggleApi()
    kaggle_api_obj.authenticate()
    return kaggle_api_obj


def lambda_handler(event, context):
    kaggle_api = get_authenticated_kaggle_api_obj()

    dataset = find_dataset(kaggle_api, event['dataset_search_name'])
    
    logger.info(f"Owner name: {dataset.ownerName}")
    logger.debug(f"Current version: {dataset.currentVersionNumber}")
    logger.info(f"Dataset size: {dataset.size}")
    logger.debug(f"Dataset size in bytes: {dataset.totalBytes}")
    
    dataset_files_list = get_dataset_files_list(kaggle_api, dataset)
     

def main():
    event = {'dataset_search_name': 'martj42/international-football-results-from-1872-to-2017'}
    context = ''
    print(f'Returned: {lambda_handler(event, context)}')
    

if __name__ == '__main__':
    main()

# algorithm:
# 1. check if only one dataset exists by given name (done: find_dataset func)
# 2. get dataset metadata (file list, anything else?)
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
