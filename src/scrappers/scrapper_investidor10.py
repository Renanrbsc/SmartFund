from bs4 import BeautifulSoup
import time
from tqdm import tqdm
from src.scrappers.model_scrapper import ModelScrapper


class ScraperInvestidor10(ModelScrapper):
    def __init__(self) -> None:
        super().__init__()
        self.url = f"{self.url}fiis/"
        self.list_fiis = list()

    def extract_main_values(self) -> list:
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
        columns = ["fii_name", "fii_type", "date_process"]
        self.save_to_csv(columns, list_fiis, "investidor10_fiis_names")

    def extract_fiis_details_properties(self, list_fiis) -> list:
        """This method will scrape the details of each individual FII"""
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

            content = self.get_page_content(f"{self.url + fii_name}")
            soup = BeautifulSoup(content, "html.parser")
            try:
                quote = (
                    soup.find("div", class_="_card cotacao")
                    .find("div", class_="_card-body")
                    .find("span")
                    .text.strip()
                )
            except Exception as e:
                print(f"\n\tError on {fii_name}: {str(e)}")
            else:
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
        self.save_to_csv(columns, list_fiis_details, r"investidor10_fiis_details")

    def save_fiis_properties(self, list_fiis_properties: list) -> None:
        columns = ["fii_name", "property_name", "state", "area", "date_process"]
        self.save_to_csv(columns, list_fiis_properties, "investidor10_fiis_properties")

    def run(self):
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
