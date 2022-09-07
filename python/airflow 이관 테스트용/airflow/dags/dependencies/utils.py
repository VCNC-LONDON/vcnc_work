import os


def get_airflow_dev_env():
    airflow_env = eval(os.environ.get("AIRFLOW_ENV", "False"))
    if not airflow_env:
        raise Exception("AIRFLOW_ENV is not True")

    dev_env = os.environ.get("DEV_ENV", "local")
    if dev_env in ["staging", "production"]:
        return dev_env
    else:
        raise Exception(f"Can't parse DEV_ENV environment variable : {dev_env}")


def read_sql(file_name: os.path):
    """sql 파일을 읽기 위한 메서드

    Args:
        file_name (os.path): 읽으려는 sql의 경로

    Returns:
        string : SQL
    """
    with open(file_name, "r") as file:
        s = file.read()
    return s
