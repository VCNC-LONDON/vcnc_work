import datetime
import pandas as pd

from google.cloud import bigquery
from dependencies.weatherforecast.weather_model import WeatherModel


class WeatherForecast(WeatherModel):
    BIGQUERY_PROJECT = "kr-co-vcnc-tada"
    BIGQUERY_DATASET = "tada_temp_london"
    BIGQUERY_TABLE = "weather_test"
    BIGQUERY_TIME_PARTITION_FIELD = "updated_at_kr"
    BIGQUERY_FIELDS = [
        "date_kr",
        "h3_l7",
        "lat",
        "lon",
        "updated_at_kr",
        "base_at_kr",
        "forecast_at_kr",
        "is_thunder",
        "precipitation",
        "rain",
        "sky_condition",
        "temperatures",
        "humidity_rate",
        "wind_spped_mps",
    ]

    def __init__(self):
        pass

    @staticmethod
    def tranform_rn1(x: str):
        result = ""
        if x == "강수없음":
            result = "01_강수없음"
        elif x == "1.0mm 미만":
            result = "02_1mm 미만"
        elif x == "30.0~50.0mm":
            result = "04_30mm 이상 50mm 미만"
        elif x == "50.0mm 이상":
            result = "05_50mm 이상"
        else:
            result = "03_1mm 이상 30mm 미만"
        return result

    def get_bigquery_info(self) -> dict:
        bigquery_info = {}
        bigquery_info["project"] = self.BIGQUERY_PROJECT
        bigquery_info["dataset"] = self.BIGQUERY_DATASET
        bigquery_info["table"] = self.BIGQUERY_TABLE
        bigquery_info["location"] = "US"
        bigquery_info["time_partition_field"] = self.BIGQUERY_TIME_PARTITION_FIELD
        bigquery_info["schema"] = [
            bigquery.SchemaField(
                "h3_l7", "STRING", mode="NULLABLE", description="H3 Level 7 index"
            ),
            bigquery.SchemaField(
                "lat", "FLOAT", mode="NULLABLE", description="H3 Level 7 중심 위도"
            ),
            bigquery.SchemaField(
                "lon", "FLOAT", mode="NULLABLE", description="H3 Level 7 중심 경도"
            ),
            bigquery.SchemaField(
                "updated_at_kr", "DATETIME", mode="NULLABLE", description="업데이트 시각(KTC)"
            ),
            bigquery.SchemaField(
                "base_at_kr",
                "DATETIME",
                mode="NULLABLE",
                description="예보가 발표된 기준 시간(KTC). 매시간 30분에 생성되고 10분마다 최신 정보로 업데이트",
            ),
            bigquery.SchemaField(
                "forecast_at_kr", "DATETIME", mode="NULLABLE", description="예보 시각(KTC)"
            ),
            bigquery.SchemaField(
                "is_thunder", "BOOLEAN", mode="NULLABLE", description="천둥 번개 여부"
            ),
            bigquery.SchemaField(
                "precipitation",
                "INTEGER",
                mode="NULLABLE",
                description="강수형태. 없음(0), 비(1), 비/눈(2), 눈(3), 빗방울(5), 빗방울눈날림(6), 눈날림(7)",
            ),
            bigquery.SchemaField(
                "rain", "STRING", mode="NULLABLE", description="1시간 강수량"
            ),
            bigquery.SchemaField(
                "sky_condition",
                "INTEGER",
                mode="NULLABLE",
                description="하늘 상태 맑음(1), 구름많음(3), 흐림(4)",
            ),
            bigquery.SchemaField(
                "temperatures", "INTEGER", mode="NULLABLE", description="기온 (섭씨)"
            ),
            bigquery.SchemaField(
                "humidity_rate", "FLOAT", mode="NULLABLE", description="습도(%)"
            ),
            bigquery.SchemaField(
                "wind_spped_mps",
                "FLOAT",
                mode="NULLABLE",
                description="풍속(m/s). 9 이상은 나뭇가지가 흔들리는 정도, 14이상은 작은 나무가 흔들리는 정도",
            ),
        ]
        return bigquery_info

    def get_url(self, request_type: str) -> str:
        """조회할 예보 정보를 input 으로 받아 endpoint 를 생성

        Args:
            request_type (str): 모델별 조회 key
              -- 예보 model : shorterm(단기 예보), hyper_shorterm(초단기 예보)
              -- 실황 model : hyper_shorterm_now(초단기 실황)

        Raises:
            Exception: 올바르지 않은 key 를 입력한 경우 raise

        Returns:
            str: endpoint
        """
        if request_type == "shorterm":  # 단기 예보
            endpoint_type = "/getVilageFcst"
        elif request_type == "hyper_shorterm":  # 초단기 예보
            endpoint_type = "/getUltraSrtFcst"
        # elif request_type == "hyper_shorterm_now": # 초단기 실황
        #     endpoint_type = "/getUltraSrtNcst"
        else:
            raise Exception("Request invalid search type")

        url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0" + endpoint_type
        return url

    def parse_response(self, data: dict) -> pd.DataFrame:
        """API response 로 전달받은 데이터를 용도에 맞게 변환하는 메서드

        Args:
            data (dict): API response 의 json 치환

        Returns:
            pd.DataFrame
        """
        items = data["response"]["body"]
        KTC = datetime.timezone(datetime.timedelta(hours=9))
        result = []

        for idx, i_value in enumerate(items["items"]["item"]):
            weather_dict = {}
            weather_dict["h3_l7"] = items["h3_l7"]
            weather_dict["lat"] = items["lat"]
            weather_dict["lon"] = items["lon"]
            weather_dict["updated_at_kr"] = items["updated_at_kr"]

            weather_dict["base_at_kr"] = datetime.datetime.fromtimestamp(
                datetime.datetime.strptime(
                    i_value["baseDate"] + i_value["baseTime"] + "00", "%Y%m%d%H%M%S"
                ).timestamp(),
                tz=KTC,
            )
            weather_dict["forecast_at_kr"] = datetime.datetime.fromtimestamp(
                datetime.datetime.strptime(
                    i_value["fcstDate"] + i_value["fcstTime"] + "00", "%Y%m%d%H%M%S"
                ).timestamp(),
                tz=KTC,
            )

            weather_dict["category"] = i_value["category"]
            weather_dict["fcstValue"] = i_value["fcstValue"]

            result.append(weather_dict)

        df = pd.DataFrame(result)
        category = df["category"].unique()

        # SettingWithCopyWarning 경고를 주는데 critical 한 건 아니라 넘어간다. https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
        target_df = df[
            ["h3_l7", "lat", "lon", "updated_at_kr", "base_at_kr", "forecast_at_kr"]
        ].drop_duplicates()

        for c_name in category:
            if c_name in ["UUU", "VVV", "VEC"]:
                pass
            else:
                df_tmp = df[df["category"] == c_name]
                df_tmp.rename(columns={"fcstValue": f"{c_name}"}, inplace=True)
                df_tmp = df_tmp.drop(columns="category")
                target_df = pd.merge(
                    target_df,
                    df_tmp,
                    on=[
                        "h3_l7",
                        "lat",
                        "lon",
                        "updated_at_kr",
                        "base_at_kr",
                        "forecast_at_kr",
                    ],
                    how="left",
                )

        target_df["LGT"] = target_df["LGT"].apply(
            lambda x: True if float(x) > 0.0 else False
        )
        target_df["PTY"] = target_df["PTY"].astype(
            "int"
        )  # 없음(0), 비(1), 비/눈(2), 눈(3), 빗방울(5), 빗방울눈날림(6), 눈날림(7)

        target_df["RN1"] = (
            target_df["RN1"].apply(WeatherForecast.tranform_rn1).astype("str")
        )
        target_df["SKY"] = target_df["SKY"].astype("int")  # 맑음(1), 구름많음(3), 흐림(4)
        target_df["T1H"] = target_df["T1H"].astype("int")
        target_df["REH"] = target_df["REH"].astype("int") / 100
        target_df["WSD"] = target_df["WSD"].astype("float")
        target_df.rename(
            columns={
                "LGT": "is_thunder",
                "PTY": "precipitation",
                "RN1": "rain",
                "SKY": "sky_condition",
                "T1H": "temperatures",
                "REH": "humidity_rate",
                "WSD": "wind_spped_mps",
            },
            inplace=True,
        )
        return target_df
