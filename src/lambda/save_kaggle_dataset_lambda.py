from kaggle import KaggleApi


kaggle_api_obj = KaggleApi()
#TODO: authenticate
kaggle_api_obj.authenticate()
print(kaggle_api_obj.dataset_list())
