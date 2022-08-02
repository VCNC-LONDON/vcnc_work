import os

def read_sql(file_name : os.path):
    """ sql 파일을 읽기 위한 메서드
    Args:
        file_name (os.path): 읽으려는 sql의 경로
    Returns:
        string : SQL
    """
    with open(file_name, 'r') as file:
        s = file.read()
    return s