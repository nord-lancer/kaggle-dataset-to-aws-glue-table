from ast import main
import sys
from kaggle import KaggleApi


kaggle_api_obj = KaggleApi()
kaggle_api_obj.authenticate()


def find_dataset(dataset_name):
    kaggle_dataset = kaggle_api_obj.dataset_list(search=dataset_name)
    if len(kaggle_dataset) > 1:
        print(f"Found {len(kaggle_dataset)} datasets:")
        for dataset in kaggle_dataset:
            print(dataset)
        print("ERROR: This API can process only one dataset at a time!")  # TODO: return this 
        print("Please input the exact name of the dataset like this: username/dataset-name.")
        return 1
    else:
        print(f"Found dataset {kaggle_dataset}!")
        return 0
    


def load_dataset_to_s3():
    pass


def load_dataset_metadata_to_s3():
    pass


def main():
    dataset_to_retrieve = "residentmario/ramen-ratings"
    dataset_to_retrieve_just_name = "ramen-ratings"
    
    find_dataset(dataset_to_retrieve)
    return 0

if __name__ == "__main__":
    sys.exit(int(main() or 0))
