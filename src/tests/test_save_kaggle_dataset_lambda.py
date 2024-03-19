from src.lambdas.save_kaggle_dataset_lambda import lambda_handler, get_files_to_download_list
from unittest import mock
from unittest.mock import patch


class DatasetFileMock:
    def __init__(self, name):
        self.name = name


def test__get_files_to_download_list__correct_files_list():
    event = {"dataset_search_name": "martj42/international-football-results-from-1872-to-2017",
            "files_to_download": ["results.csv", "shootouts.csv"]}
    dataset_files = [DatasetFileMock("results.csv"), 
                     DatasetFileMock("shootouts.csv"),
                     DatasetFileMock("goalscorers.csv")]
    
    test_result = get_files_to_download_list(event, dataset_files)
    
    assert set(test_result) == set(dataset_files[0:2])


def inc(x):
    return x + 1


def test_answer():
    assert inc(4) == 5
    
