import sys
import logging
from pathlib import Path
from kaggle import KaggleApi


KAGGLE_DATASET_NAME_SLASH = '/'
MAX_FILE_SIZE_BYTES = 80_000_000


logger = logging.getLogger("Lambda")
logging.basicConfig()
logs_level = "DEBUG"
logger.setLevel(logs_level)
logger.info(f"Logger initialized. Set level to: {logs_level}")


class MoreThanOneDatasetFoundError(Exception):
    pass


class RequestedFilesNotFoundError(Exception):
    def __init(self, files_not_found, *args, **kwargs):
        self.files_not_found = files_not_found


class RequestedFilesTooBig(Exception):
    pass


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
    dataset_all_file_names = {file.name:file for file in dataset_files}
        
    # iterate through each file when searching
    # if some files are not found - we need to record that and return 
    # that list to the user
    # todo: handle files not found
    files_not_found = list()
    files_found = list()
    for i in files_to_download:
        if i in dataset_all_file_names.keys():
            logging.debug(f"File {i} found.")
            files_found.append(dataset_all_file_names[i])
            continue
        
        file_name_no_suffix = Path(i).stem
        dataset_files_no_suffix = {Path(file.name).stem:file for \
                                   file in dataset_files}
        if file_name_no_suffix in dataset_files_no_suffix.keys():
            logging.debug(f"File {i} found.")
            files_found.append(dataset_files_no_suffix[i])
            continue
        files_not_found.append(i)
    
    if files_not_found:
        for i in files_not_found:
            logging.error(f"ERROR: File {i} was not found!")
        raise RequestedFilesNotFoundError(files_not_found,
            "ERROR: Some of the requested files were not found!")
        
    return files_found


def check_only_one_dataset_found(dataset_list):
    if len(dataset_list) > 1: # put in a separate function?
        logger.error(f"ERROR: Found more than one dataset:")
        for dataset in dataset_list:
            logger.error(dataset)
        
        raise MoreThanOneDatasetFoundError(
            "ERROR: This API can process only one dataset at a time!"
            "Please input the exact name of the dataset like this: "
            "username/dataset-name.")
    return 0 # todo: maybe return something else?


def check_requested_files_within_size_limit(requested_files):
    pass


def check_requested_file_formats_supported(requested_files):
    pass 


def get_authenticated_kaggle_api_obj():
    kaggle_api_obj = KaggleApi()
    kaggle_api_obj.authenticate()
    return kaggle_api_obj


def lambda_handler(event, context):
    kaggle_api = get_authenticated_kaggle_api_obj()

    dataset_list = find_dataset(kaggle_api, event['dataset_search_name'])

    # 1. check if dataset is eligible:    
    #   a. check if only one dataset exists by given name (done: find_dataset func)
    #   b. check if dataset is less than 80MB (for now)
    #   c. get dataset metadata (only file list for now, maybe will add more things in the future)
    #   d. check if file types are supported by Athena (.avro, .parquet, .csv, .tsv, .json, .orc)

    # todo: add try-except here
    check_only_one_dataset_found(dataset_list)
    kaggle_dataset = dataset_list[0]
    dataset_files_list = get_dataset_files_list(kaggle_api, kaggle_dataset)
    
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
