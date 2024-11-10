import os
import csv
import requests
from datetime import datetime


class ModelScrapper:
    def __init__(self) -> None:
        self.headers = {"User-Agent": "Mozilla/5.0"}
        self.now = datetime.now().date()
        self.url = "https://investidor10.com.br/"
        self.save_folder = os.path.join(
            os.path.dirname(__file__), "..", "..", "Datasets", "Ingest", "Investidor10",
        )

    def get_page_content(self, url):
        response = requests.get(url, headers=self.headers)
        return response.content

    def save_to_csv(self, columns: list, values: list, name_file: str):
        with open(
            os.path.join(self.save_folder, f"{name_file}.csv"),
            "w",
            newline="",
            encoding="utf-8",
        ) as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(values)
