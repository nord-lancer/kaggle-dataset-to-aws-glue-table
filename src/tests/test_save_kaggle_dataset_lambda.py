# test stub
from src.lambdas.save_kaggle_dataset_lambda import lambda_handler


def lambda_handler_test():
    event = {'dataset_name': 'martj42/international-football-results-from-1872-to-2017'}
    context = ''
    
    assert lambda_handler(event, context) == 0


def inc(x):
    return x + 1


def answer_test():
    assert inc(4) == 5
    
