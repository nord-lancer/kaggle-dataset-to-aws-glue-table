from ast import main
from math import log
import sys
import logging
from kaggle import KaggleApi


KAGGLE_DATASET_NAME_SLASH = '/'
MAX_FILE_SIZE_BYTES = 80_000_000


logger = logging.getLogger("Lambda")
logging.basicConfig()
logs_level = "DEBUG"
logger.setLevel(logs_level)
logger.info(f"Logger initialized. Set level to: {logs_level}")


def find_dataset(kaggle_api,dataset_name):
    kaggle_dataset = kaggle_api.dataset_list(search=dataset_name)
    logger.info(f"Found dataset(s): .")
    return kaggle_dataset  
    

def get_dataset_files_list(kaggle_api, dataset):
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


def get_files_to_download_list(event, dataset_files):
    if len(dataset_files) == 1:
        logging.debug("Dataset contains only one file. Processing it...")
        return dataset_files
    
    files_to_download = event["files_to_download"]
    dataset_file_names_list = [x.name for x in dataset_files]
        
    # 1. search normally
    if set(files_to_download).issubset(dataset_file_names_list):
        logging.info("Found all requested files withing the dataset.")
        
        return [x for x in dataset_files if x.name in files_to_download]
    # 2. search, but without file extensions 
    # todo: return requested list, but take names from dataset_files  
    # each requested file must be found or throw an error 


def check_dataset_eligibility(kaggle_api, event, dataset_list):
    # 1. check if dataset is eligible:    
    #   a. check if only one dataset exists by given name (done: find_dataset func)
    #   b. check if dataset is less than 80MB (for now)
    #   c. get dataset metadata (only file list for now, maybe will add more things in the future)
    #   d. check if there is more than 1 file (if files to download is not provided - ask for files)
    #       if dataset has only 1 file - just make a table out of that file, 
    #   e. check if file types are supported by Athena (.avro, .parquet, .csv, .tsv, .json, .orc)
    if len(dataset_list) > 1:
        logger.error(f"ERROR: Found more than one dataset:")
        for dataset in dataset_list:
            logger.error(dataset)
        
        # TODO: return this message from the API
        logger.error("ERROR: This API can process only one dataset at a time!") 
        logger.error("Please input the exact name of the dataset like this: "
                     "username/dataset-name.")
        return 1
    
    kaggle_dataset = dataset_list[0]
    dataset_files_list = get_dataset_files_list(kaggle_api, kaggle_dataset)
    
    if kaggle_dataset.totalBytes > MAX_FILE_SIZE_BYTES:  # Wrong: need to check each file instead
        logger.error("ERROR: Dataset is too big!")
    
    if "number of files":
        pass
    if "supported file formats":
        pass
        
    return kaggle_dataset


def get_authenticated_kaggle_api_obj():
    kaggle_api_obj = KaggleApi()
    kaggle_api_obj.authenticate()
    return kaggle_api_obj


def lambda_handler(event, context):
    kaggle_api = get_authenticated_kaggle_api_obj()

    dataset_list = find_dataset(kaggle_api, event['dataset_search_name'])
    
    dataset = check_dataset_eligibility(kaggle_api, event, dataset_list)
    
    #logger.info(f"Owner name: {dataset.ownerName}")
    #logger.debug(f"Current version: {dataset.currentVersionNumber}")
    #logger.info(f"Dataset size: {dataset.size}")
    #logger.debug(f"Dataset size in bytes: {dataset.totalBytes}")
     

def main():
    event = {"dataset_search_name": "martj42/international-football-results-from-1872-to-2017",
             "files_to_download": ["results.csv", "shootouts.csv"]}
    context = ""
    print(f"Returned: {lambda_handler(event, context)}")
    

if __name__ == '__main__':
    main()

# algorithm:
# 1. check if dataset is eligible  
# 2. check if event has S3 location for data
# 3. if event doesn't have S3 location, check if env variable has S3 data location set
# 4. place dataset data and on S3 (location either from env var or event, 
# event location always in priority)
# don't forget about compression: https://docs.aws.amazon.com/databrew/latest/dg/supported-data-file-sources.html    
# 5. make sure that data is placed on S3 successfully
# 6. generate athena query using table metadata (from glue crawler?)
# 7. run athena query to create a glue table (or maybe use only glue?)
# 8. check if glue table was properly created (all column are present, all data types
# are correct, amount of rows is correct)
#
# TODO: think about processing files without extension in the name    
