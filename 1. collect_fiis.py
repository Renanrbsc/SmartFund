from src.scrappers.scrapper_investidor10 import ScraperInvestidor10


class SmartFund:
    def run(self):
        print("\n\tWelcome to the SmartFund scraper!")
        scraper = ScraperInvestidor10()
        scraper.run()


if __name__ == "__main__":
    main = SmartFund()
    main.run()
