import kagglehub
# Download latest version
def download_kaggle_dataset(dataset_name: str) -> str:
    # "devarajv88/target-dataset"
    dowload_path = kagglehub.dataset_download(dataset_name)
    return dowload_path
