from utils.Utils import Utils
import requests
import pandas as pd
import datetime
import os
from interfaces.EnumBuckets import EnumBuckets


class OpenMeteoRaw:
    def __init__(self):
        self.utils = Utils()
        self.spark = self.utils.init_spark()
        self.bucket_name = EnumBuckets.RAW.value
        self.api_base = "https://api.open-meteo.com/v1/forecast"
        self.variables = ["temperature_2m", "rain", "wind_speed_10m"]

    def get_data(self, latitude, longitude, start_date=None, end_date=None):
        if start_date is None:
            start_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.date.today().strftime("%Y-%m-%d")

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(self.variables),
            "timezone": "America/Sao_Paulo",
            "start_date": start_date,
            "end_date": end_date
        }

        from urllib.parse import urlencode
        url = f"{self.api_base}?{urlencode(params)}"
        print(f"URL final da requisição: {url}")

        response = requests.get(self.api_base, params=params)
        response.raise_for_status()
        return response.json()

    def process_and_upload(self, latitude, longitude, start_date=None, end_date=None):
        data = self.get_data(latitude, longitude, start_date, end_date)
        hourly = data["hourly"]

        df = pd.DataFrame(hourly)
        df["latitude"] = latitude
        df["longitude"] = longitude
        df["data_extracao"] = datetime.datetime.now().isoformat()

        spark_df = self.spark.createDataFrame(df)

        prefix = "openmeteo"
        sensor = "clima"

        os.makedirs("temp", exist_ok=True)
        csv_path = self.utils.transform_df_to_csv(spark_df, sensor, prefix)


        self.utils.set_data_s3_file(csv_path, self.bucket_name)

        print("Dados do Open Meteo enviados para camada RAW com sucesso!\n")


if __name__ == "__main__":
    coleta = OpenMeteoRaw()
    coleta.process_and_upload(latitude=-23.56, longitude=-46.65)
