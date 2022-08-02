import requests
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map
from sql import query_factory
from utils import bigquery
import multiprocessing as mp
import numpy as np

bq = bigquery.operator()

def inavi(start_lng, start_lat, end_lng, end_lat, wp1_lng, wp1_lat, wp2_lng, wp2_lat, coordType = "wgs84",option = "recommendation",usageType = 1,useAngle = True) :

    appkey = "30utUTE2dUUsRFgMMzzk"
    query = f"https://api-maps.cloud.toast.com/maps/v3.0/appkeys/{appkey}/route-normal?option={option}&coordType={coordType}&usageType={usageType}&useAngle={useAngle}&startX={start_lng}&startY={start_lat}&endX={end_lng}&endY={end_lat}&v1a1X={wp1_lng}&via1Y={wp1_lat}&via2X={wp2_lng}&via2Y={wp2_lat}"

    r = requests.get(query).json()
        
    if r['header']['resultCode'] == 0 :
        res = {
            'dist' : r['route']['data']['distance'],
            'time' : r['route']['data']['spend_time'],
            'code' : r['header']['resultCode'],
            'msg' : r['header']['resultMessage']
        }
    else :
        res = {
            'dist' : "",
            'time' : "",
            'code' : r['header']['resultCode'],
            'msg' : r['header']['resultMessage']
        }

    return res

def inavi_processing(data):
    tqdm.pandas()
    
    data['res'] = data.progress_apply(
        lambda x :inavi(
            x['approxy_driver_lng'],
            x['approxy_driver_lat'],
            x['origin_lng'],
            x['origin_lat'],
            x['destination_lng'],
            x['destination_lat'],
            x['start_lng'],
            x['start_lat']
        ),
        axis = 1
    )
    data = data.astype({'res':'str'})
    return data

def processing(data, nums):
    data_split = np.array_split(data, nums)
    results = []
    
    pool = mp.Pool(nums)
    # res = process_map(inavi_processing, data_split, max_workers= nums)
    # with mp.Pool(nums) as pool :
    #     res = list(tqdm(pool.imap_unordered(inavi_processing, data_split), total=len(data)))
    # res = pool.map(inavi_processing, data_split)
    # pool.close()
    # return pd.concat(list(res))

    with mp.Pool(processes=nums) as p:
        with tqdm(total=nums, desc='Parallel Processing') as pbar:
            for result in p.imap_unordered(inavi_processing, data_split):
                pbar.update()
                results.append(result)
                
    return pd.concat(results)

if __name__ == "__main__" :
    print(" ")
    print("##### 1. 프로세스를 시작합니다.")
    raw = bq.read_bq(query_factory.get_raw_data_sql())
    print(" ")
    print(raw.head(3))
    print(" ")
    
    print("##### 2. 데이터 로딩 완료. 아이나비 API 를 적용합니다.")
    p_raw = processing(raw, 8)
    print(p_raw.head(3))
    print(" ")
    
    print("##### 3. 아이나비 API 적용 완료. 데이터를 빅쿼리에 저장합니다.")
    bq.update_bq(p_raw , "tada_temp_london.realtime_in_reservation_fev")
    print(" ")
    print("##### 4. 모든 프로세스가 종료되었습니다.")
