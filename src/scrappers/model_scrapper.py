import os
import csv
import requests
from datetime import datetime


class ModelScraper:
    """
    A class for scraping data and saving it in CSV format.

    Attributes:
        headers (dict): HTTP headers for web requests.
        now (datetime.date): The current date.
        url (str): Base URL for scraping.
        save_folder (str): Path to the folder where CSV files are saved.
    """

    def __init__(self) -> None:
        """
        Initializes the ModelScraper with default headers, the current date,
        a base URL, and the folder path for saving datasets.
        """
        self.headers = {"User-Agent": "Mozilla/5.0"}
        self.now = datetime.now().date()
        self.url = "https://investidor10.com.br/"
        self.save_folder = os.path.join(
            os.path.dirname(__file__), "..", "..", "Datasets", "Ingest", "Investidor10"
        )

    def get_page_content(self, url: str) -> bytes:
        """
        Fetches the content of a webpage.

        Args:
            url (str): The URL of the webpage to fetch.

        Returns:
            bytes: The raw content of the webpage.
        """
        response = requests.get(url, headers=self.headers, timeout=(5, 10))
        response.raise_for_status()
        return response.content

    def save_to_csv(self, columns: list, values: list, file_name: str) -> None:
        """
        Saves data to a CSV file.

        Args:
            columns (list): A list of column headers.
            values (list): A list of rows to save in the CSV file.
            file_name (str): The name of the file (without extension).

        Returns:
            None
        """
        os.makedirs(self.save_folder, exist_ok=True)

        file_path = os.path.join(self.save_folder, f"{file_name}.csv")

        with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(values)
