from bs4 import BeautifulSoup
import time
from tqdm import tqdm
from requests.exceptions import ChunkedEncodingError, RequestException
from src.scrappers.model_scrapper import ModelScraper
import requests


class ScraperInvestidor10(ModelScraper):
    """
    A scraper for fetching data from the "Investidor10" website, specifically focused on FIIs (Real Estate Investment Trusts).

    This class extends the `ModelScraper` base class and customizes it for scraping information about FIIs from the Investidor10 website.

    Attributes:
        url (str): The base URL for FIIs data on the Investidor10 website.
        list_fiis (list): A list to store data related to FIIs.
    """

    def __init__(self) -> None:
        """
        Initializes the ScraperInvestidor10 with the specific URL for FIIs data
        and an empty list for storing FIIs information.
        """
        super().__init__()
        self.url = f"{self.url}fiis/"
        self.list_fiis = list()

    def get_page_content(self, url: str, retries: int = 5, timeout: int = 10) -> str:
        """
        Makes a request to a webpage and returns its content, with support for retries in case of failures.

        Args:
            url (str): The URL of the webpage to fetch.
            retries (int): The number of retry attempts in case of failure. Default is 5.
            timeout (int): The timeout duration for the request in seconds. Default is 10.

        Returns:
            str: The content of the webpage as a string.

        Raises:
            RequestException: If all retry attempts fail.
        """
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=timeout)
                response.raise_for_status()
                return response.text
            except (ChunkedEncodingError, RequestException) as e:
                print(f"Error on attempt {attempt + 1} for {url}: {e}")
                if attempt < retries - 1:
                    import time

                    time.sleep(2)
                else:
                    raise e

    def extract_main_values(self) -> list:
        """
        Extracts FII names and types from multiple pages on the Investidor10 website.

        Returns:
            list: A list of extracted FIIs data, each entry containing:
                - FII name (str)
                - FII type (str, default "Fii")
                - Extraction date (datetime.date)
        """
        page = 1

        while True:
            url = f"{self.url}?page={page}"
            content = self.get_page_content(url)
            soup = BeautifulSoup(content, "html.parser")

            # Extract FII values
            fiis = soup.find_all("div", class_="actions-card")
            if not fiis:
                break

            for fii in tqdm(fiis, desc=f"Extracting FIIs Page {page}"):
                fii_name = fii.find("h2", class_="ticker-name").text.strip()
                fii_type = "Fii"
                self.list_fiis.append([fii_name, fii_type, self.now])
            page += 1
            time.sleep(2)
        return self.list_fiis

    def save_fiis_main(self, list_fiis: list) -> None:
        """
        Saves the list of FIIs data to a CSV file.

        Args:
            list_fiis (list): A list of FIIs data, where each entry contains:
                - FII name (str)
                - FII type (str)
                - Extraction date (datetime.date)
        """
        columns = ["fii_name", "fii_type", "date_process"]
        self.save_to_csv(columns, list_fiis, "investidor10_fiis_names")

    def extract_fiis_details_properties(self, list_fiis) -> list:
        """
        Extracts detailed information about each FII (Real Estate Investment Trust) and its properties
        from the Investidor10 website. This method scrapes multiple attributes for each FII, including
        financial metrics, general details, and property-related data.

        It iterates over the provided list of FIIs, accessing individual pages for each FII to gather:
        - Financial indicators like quote, dividend yield, price-to-book ratio, and liquidity.
        - Additional information such as appreciation over the past 12 months, vacancy rate,
        number of unit holders, issued units, net asset value per unit, last dividend payment,
        corporate name, CNPJ (company registration number), target audience, investment objectives,
        market sector, fund type, fund term, and management fee.
        - Property details, including property name, state, and area, for each FII listed on the
        Investidor10 platform.

        The extracted data is then compiled into two separate lists:
        1. `list_fiis_details`: Contains detailed information about each FII.
        2. `list_fiis_properties`: Contains details of the properties associated with each FII.

        Args:
            list_fiis (list): A list of FIIs data, where each entry contains the name of an individual FII.

        Returns:
            tuple: A tuple containing two lists:
                - list_fiis_details (list): A list of detailed attributes for each FII, where each entry is a list containing:
                    - FII name (str)
                    - Quote (str or None)
                    - Dividend yield (str or None)
                    - Price-to-book ratio (str or None)
                    - Liquidity (str or None)
                    - Appreciation in 12 months (str or None)
                    - Vacancy (str or None)
                    - Number of unit holders (str or None)
                    - Issued units (str or None)
                    - Net asset value per unit (str or None)
                    - Net asset value (str or None)
                    - Last dividend payment (str or None)
                    - Corporate name (str or None)
                    - CNPJ (str or None)
                    - Target audience (str or None)
                    - Investment objective (str or None)
                    - Market sector (str or None)
                    - Fund type (str or None)
                    - Fund term (str or None)
                    - Management type (str or None)
                    - Management fee (str or None)
                    - Extraction date (datetime.date)
                - list_fiis_properties (list): A list of properties associated with each FII, where each entry is a list containing:
                    - FII name (str)
                    - Property name (str)
                    - State (str)
                    - Area (str)
                    - Extraction date (datetime.date)
        """
        list_fiis_details = list()
        list_fiis_properties = list()

        for fii in tqdm(list_fiis, desc="Extracting FIIs Details"):
            fii_name = fii[0]
            quote = None
            dividend_yield = None
            price_book_ratio = None
            liquidity = None
            appreciation_12months = None
            vacancy = None
            number_unit_holders = None
            issued_units = None
            net_asset_value_per_unit = None
            net_asset_value = None
            last_dividend_payment = None
            corporate_name = None
            cnpj = None
            target_audience = None
            invest_objective = None
            market_sector = None
            fund_type = None
            fund_term = None
            management_type = None
            management_fee = None

            try:
                content = self.get_page_content(f"{self.url + fii_name}")
                soup = BeautifulSoup(content, "html.parser")
            except Exception as e:
                print(f"\n\tError on {fii_name}: {str(e)}")
            else:
                quote = (
                    soup.find("div", class_="_card cotacao")
                    .find("div", class_="_card-body")
                    .find("span")
                )
                if quote:
                    quote = quote.text.strip()

                find_dividend_yield = (
                    soup.find_all("div", class_="_card dy")[0]
                    .find("div", class_="_card-body")
                    .find("span")
                )
                if find_dividend_yield:
                    dividend_yield = find_dividend_yield.text.strip()

                find_price_book_ratio = (
                    soup.find("div", class_="_card vp")
                    .find("div", class_="_card-body")
                    .find("span")
                )
                if find_price_book_ratio:
                    price_book_ratio = find_price_book_ratio.text.strip()

                find_liquidity = (
                    soup.find("div", class_="_card val")
                    .find("div", class_="_card-body")
                    .find("span")
                )
                if find_liquidity:
                    liquidity = find_liquidity.text.strip()

                find_appreciation_12months = (
                    soup.find_all("div", class_="_card dy")[1]
                    .find("div", class_="_card-body")
                    .find("span")
                )
                if find_appreciation_12months:
                    appreciation_12months = find_appreciation_12months.text.strip()

                for div_desc in soup.find_all("div", class_="desc"):
                    if "VACÂNCIA" in div_desc.text:
                        vacancy = div_desc.find("div", class_="value").text.strip()
                    if "NUMERO DE COTISTAS" in div_desc.text:
                        number_unit_holders = div_desc.find(
                            "div", class_="value"
                        ).text.strip()
                    if "COTAS EMITIDAS" in div_desc.text:
                        issued_units = div_desc.find("div", class_="value").text.strip()
                    if "VAL. PATRIMONIAL P/ COTA" in div_desc.text:
                        net_asset_value_per_unit = div_desc.find(
                            "div", class_="value"
                        ).text.strip()
                    if "VALOR PATRIMONIAL" in div_desc.text:
                        net_asset_value = div_desc.find(
                            "div", class_="value"
                        ).text.strip()
                    if "ÚLTIMO RENDIMENTO" in div_desc.text:
                        last_dividend_payment = div_desc.find(
                            "div", class_="value"
                        ).text.strip()
                    if "Razão Social" in div_desc.text:
                        corporate_name = div_desc.find(
                            "div", class_="value"
                        ).text.strip()
                    if "CNPJ" in div_desc.text:
                        cnpj = div_desc.find("div", class_="value").text.strip()
                    if "PÚBLICO-ALVO" in div_desc.text:
                        target_audience = div_desc.find(
                            "div", class_="value"
                        ).text.strip()
                    if "MANDATO" in div_desc.text:
                        invest_objective = div_desc.find(
                            "div", class_="value"
                        ).text.strip()
                    if "SEGMENTO" in div_desc.text:
                        market_sector = div_desc.find(
                            "div", class_="value"
                        ).text.strip()
                    if "TIPO DE FUNDO" in div_desc.text:
                        fund_type = div_desc.find("div", class_="value").text.strip()
                    if "PRAZO DE DURAÇÃO" in div_desc.text:
                        fund_term = div_desc.find("div", class_="value").text.strip()
                    if "TIPO DE GESTÃO" in div_desc.text:
                        management_type = div_desc.find(
                            "div", class_="value"
                        ).text.strip()
                    if "TAXA DE ADMINISTRAÇÃO" in div_desc.text:
                        management_fee = div_desc.find(
                            "div", class_="value"
                        ).text.strip()

            list_fiis_details.append(
                [
                    fii_name,
                    quote,
                    dividend_yield,
                    price_book_ratio,
                    liquidity,
                    appreciation_12months,
                    vacancy,
                    number_unit_holders,
                    issued_units,
                    net_asset_value_per_unit,
                    net_asset_value,
                    last_dividend_payment,
                    corporate_name,
                    cnpj,
                    target_audience,
                    invest_objective,
                    market_sector,
                    fund_type,
                    fund_term,
                    management_type,
                    management_fee,
                    self.now,
                ]
            )

            # Extract property information
            properties = soup.find("div", id="container-properties")

            if properties is not None:
                properties = properties.find_all("div", class_="card-propertie")

                # Iterate over the properties and extract the information
                for property in properties:
                    # Extract property details
                    property_name = property.find("h3").text.strip()

                    details = property.find_all("small")
                    state = details[0].text.strip()
                    area = details[1].text.strip()

                    # Append the extracted values to the "values" list
                    list_fiis_properties.append(
                        [fii_name, property_name, state, area, self.now]
                    )

            time.sleep(2)
        return list_fiis_details, list_fiis_properties

    def save_fiis_details(self, list_fiis_details: list) -> None:
        """
        Saves detailed FII information to a CSV file.

        Args:
            list_fiis_details (list): A list of detailed FII data.
        """
        columns = [
            "fii_name",
            "quote",
            "dividend_yield",
            "price_book_ratio",
            "liquidity",
            "appreciation_12months",
            "vacancy",
            "number_unit_holders",
            "issued_units",
            "net_asset_value_per_unit",
            "net_asset_value",
            "last_dividend_payment",
            "corporate_name",
            "cnpj",
            "target_audience",
            "invest_objective",
            "market_sector",
            "fund_type",
            "fund_term",
            "management_type",
            "management_fee",
            "date_process",
        ]
        self.save_to_csv(columns, list_fiis_details, "investidor10_fiis_details")

    def save_fiis_properties(self, list_fiis_properties: list) -> None:
        """
        Saves FII property information to a CSV file.

        Args:
            list_fiis_properties (list): A list of FII property data.
        """
        columns = ["fii_name", "property_name", "state", "area", "date_process"]
        self.save_to_csv(columns, list_fiis_properties, "investidor10_fiis_properties")

    def run(self):
        """
        Executes the full scraping and saving process for FIIs data.

        This method coordinates the entire process of collecting and saving FII data:
        - Extracts main FII information.
        - Extracts detailed FII information and property data.
        - Saves both sets of data to CSV files.
        """
        print("Started collect all Fii details!")
        print("Wait to finish process!")

        list_fiis = self.extract_main_values()
        self.save_fiis_main(list_fiis)
        (
            list_fiis_details,
            list_fiis_properties,
        ) = self.extract_fiis_details_properties(list_fiis)
        self.save_fiis_details(list_fiis_details)
        self.save_fiis_properties(list_fiis_properties)

        print("\n\tProcess finished!")
