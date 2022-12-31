# pcs-scraper
Scrapes pro cycling stats for rider info. This provides a class and a cli tool. 

The tool scrapes all the rider stats and all their race placings. 


# setup 

COMING SOON!
Install the necessary dependencies by running pip install -r requirements.txt.

### How to use the cli tool 

 Run the scraper using the following command:

 ```python cli.py [--riders RIDERS] [--teams TEAMS] YEARS [YEARS ...] ```

Where RIDERS is the name of one or more riders to scrape data for (separated by spaces), TEAMS is the name of one or more teams to scrape data for (separated by spaces), and YEARS is one or more years to scrape data for (separated by spaces).

For example, to scrape data for Geraint Thomas for the years 2020 and 2021, you would run:

 ```python cli.py --riders geraint-thomas 2020 2021 ```

To scrape data for the Team Ineos riders in 2022 for the years 2020 and 2021, you would run:

```python cli.py --teams ineos-grenadiers-2022 2020 2021```


### How to use the Scraper class

Import the Scraper class by adding from ``from scrape import Scraper`` to the top of your Python script.

Create an instance of the Scraper class by calling ``Scraper(rider_url)``, where rider_url is the base URL of the rider's page on procyclingstats.com. For example, to scrape data for Geraint Thomas, you would use ``s = Scraper('https://www.procyclingstats.com/rider/geraint-thomas')``.

Use the ``scrape_homepage_to_parquet()`` method to scrape data from the rider's homepage and save it as a parquet file in the ../data/<name>/ directory, where <name> is the name of the rider.

Use the ``scrape_year_to_parquet(years)`` method to scrape data for a specific year or list of years and save it as a parquet file in the ../data/<name>/ directory. The years argument should be a string or a list of strings, and should be in the format 'YYYY'. For example, to scrape data for the years 2020 and 2021, you would use ``scrape_year_to_parquet(['2020', '2021'])``.

# Coming soon 

* unit tests 
* csv, postgres and parquet options for download 
* make pip installable 
