from datetime import datetime, timedelta

from airflow.models import DAG

from dependencies.weatherforecast.WeatherForecast import WeatherForecast
from dependencies.weatherforecast.weather_operator import WeatherForecastToBigQueryOperator

default_args = {
    "owner": "london", 
    "depends_on_past": False,
    "email": ["london@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "gcp_conn_id": "google_cloud_default",
    "zendesk_conn_id": "zendesk_conn",
}

with DAG(
    dag_id="Extract-hyper-shorterm-weather-forecast",
    description="load hypershorterm weather forecast data to BigQuery",
    start_date=datetime(2022, 9, 30),
    schedule_interval="50 0,2,4,6,8,10,12,14,18 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    weather_forecast = WeatherForecast()
    weather_forecast_operator = WeatherForecastToBigQueryOperator(
        task_id="hyper_shorterm_weather_forecast",
        weatherforecast_service_type=weather_forecast,
        weatherforecast_request_type="hyper_shorterm",
    )
