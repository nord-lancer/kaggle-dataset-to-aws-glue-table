# test stub
from src.lambdas.save_kaggle_dataset_lambda import lambda_handler, get_files_to_download_list


def test_get_files_to_download_list():
    event = {"dataset_search_name": "martj42/international-football-results-from-1872-to-2017",
            "files_to_download": ["results.csv", "shootouts.csv"]}
    context = {}
    class DatasetFileMock:
        def __init__(self, name):
            self.name = name
    dataset_files = [DatasetFileMock("results.csv"), DatasetFileMock("shootouts.csv")]

    get_files_to_download_list(event, dataset_files)
    

def inc(x):
    return x + 1


def test_answer():
    assert inc(4) == 5
    
