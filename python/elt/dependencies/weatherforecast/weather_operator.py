from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults

from dependencies.weatherforecast.utils.bigquery_loader import BigqueryLoader


class WeatherForecastToBigQueryOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        gcp_conn_id,
        weatherforecast_service_type,  # 예보 / 실황 모델중 선택
        weatherforecast_request_type,  # 단기 예보, 초단기 예보, 초단기 실황 선택
        *args,
        **kwargs,
    ):
        """BaseOperator 기반 날씨 예보 ETL operator

        Args:
            gcp_conn_id(object) : gcp connection credential
            weatherforecast_service_type(object) : operator 가 실행할 model을 인수로 받음. 예보 model (WeatherForecast) 와 실황 model (WeatherNow) 2가지가 존재
            weatherforecast_request_type(str) : 각각의 model 이 요구하는 인수
              -- 예보 model : shorterm(단기 예보), hyper_shorterm(초단기 예보)
              -- 실황 model : hyper_shorterm_now(초단기 실황)
        """
        super(WeatherForecastToBigQueryOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.weatherforecast_service_type = weatherforecast_service_type
        self.weatherforecast_request_type = weatherforecast_request_type

    def execute(self, context):
        self.log.info(f"start execute...")
        try:
            weatherforecast_api_token = Variable.get("weatherforecast_conn")
            gcp_conn = GoogleCloudBaseHook(gcp_conn_id=self.gcp_conn_id)
            google_credentials = gcp_conn._get_credentials()

            # execution date 는 2 version 부터 권고사항. 1에는 logical date 가 없다.
            execution_datetime = context.get("logical_date")
            # execution_datetime = context.get("execution_date")

            self.log.info(f"execute(): execution_date [{str(execution_datetime)}]")

            weatherforecast_to_bigquery_loader = BigqueryLoader(
                execution_datetime, weatherforecast_api_token, google_credentials
            )
            weatherforecast_to_bigquery_loader.do_etl(
                request_type=self.weatherforecast_request_type,
                target_model=self.weatherforecast_service_type,
            )
        except Exception as e:
            self.log.exception(e)
            raise
