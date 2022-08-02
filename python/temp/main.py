# from util import utils
from datetime import datetime
import os

# sql = utils.read_sql(os.path.join(os.getcwd(), 'test.sql')).format(target_date = "2022-05-01")

if __name__ == "__main__":
    # print(os.getcwd())
    # print(sql)
    print(
        int(datetime.utcnow().strftime("%H"))
        )