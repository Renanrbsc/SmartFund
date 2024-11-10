SmartFund Repository

Investment Insight: Analysis, Tax Calculation Tax; Scoring  This repository provides Python scripts and Jupyter notebooks for stock market investment analysis. Key features include data processing, tax calculation, and score generation for daily fiis. Offering tools for efficient data handling and insights into investment performance.

The code is developed in Python and makes use of libraries from requests, manipulation data and visualization data.


Getting Started

1. git clone repo
2. virtualenv venv
3. cd venv/bin/activate
4. pip install -r requirements.txt

The requirements.txt file contains a list of the required libraries along with their versions.


Project Structure
The project consists of the following files:

1. collect_fiis.py: This script collects FIIs from the Investidor10 website and saves them to a CSV file.
2. analysis_fiis.ipynb: This script analyzes the collected FIIs and generates a report based on their performance.
3. treat_b3.ipynb: This script treating of B3 data, cleaning, and saving it to a CSV file.
4. my_wallet.py: This script provides a simple implementation of a portfolio management system.
5. dashboards.py: This script generates various dashboards using Plotly and Dash. (Not working currently)
