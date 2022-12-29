import pandas as pd
import datetime as dt
import os
import re

from utils import string_fmt, title_fmt, get_soup



class Scraper:
    def __init__(self, rider_url: str):
        """class to scrape rider data from procyclingstats.com

        Args:
            rider_url (str): base url of the rider. e.g. https://www.procyclingstats.com/rider/geraint-thomas
        """
        self.url = rider_url
        self.name = rider_url.split("/")[-1]
        self.create_storage_folder()


    def create_storage_folder(self):
        if not os.path.exists(f"../data/{self.name}"):
            os.makedirs(f"../data/{self.name}")
            

    def scrape_homepage_to_parquet(self):
        df = self.get_rider_homepage_stats()
        df.to_parquet(f"../data/{self.name}/stats.parquet")

    def scrape_year_to_parquet(self, years: str | list[str]):
        if isinstance(years, str):
            years = [years]
        for year in years:
            df = self.scrape_rider_year(year)
            df.to_parquet(f"../data/{self.name}/{year}.parquet")
        

    def get_rider_homepage_stats(self) -> pd.DataFrame:
        rider_dict = {}
        soup = get_soup(self.url)
        rider_dict["name"] = title_fmt(soup.find("title").text)
        rider_dict["team"] = string_fmt(soup.find("span", class_="red").text)
        info = soup.find("div", {"class": "rdr-info-cont"})
        list_birthdate = info.contents[1:4]
        rider_dict["dob"] = dt.datetime.strptime(
            list_birthdate[0] + list_birthdate[2][:-5], " %d %B %Y"
        ).date()
        rider_dict["country"] = string_fmt(
            info.find("a", class_="black")
            .text.encode("latin-1", "ignore")
            .decode("utf-8", "ignore")
        )
        rider_dict["height"] = float(info.find(text="Height:").next.split()[0])
        rider_dict["weight"] = float(info.find(text="Weight:").next.split()[0])

        points = soup.find_all("div", {"class": "pnt"}) 
        ranks = soup.find_all("div", {"class": "rnk"})  
        titles_pnts = soup.find_all("div", {"class": "title"})[0 : len(points)]
        titles_ranks = soup.find_all("div", {"class": "title"})[
            len(points) : len(points) + len(ranks)
        ]

        for ind, title in enumerate(titles_pnts):
            rider_dict[string_fmt(title.text)] = int(points[ind].text)

        for ind, title in enumerate(titles_ranks):
            rider_dict[string_fmt(title.text)] = int(ranks[ind].text)

        rider_dict["date_scraped"] = dt.datetime.now().strftime("%d/%m/%Y-%H:%M:%S")
        return pd.DataFrame.from_dict(rider_dict, orient="index").transpose()

    @staticmethod
    def find_race_url(s: str):
        pattern = 'href="(.+?)"'
        match = re.search(pattern, s)
        if match:
            return match.group(1)

    @staticmethod   
    def scrape_race_info(url: str) -> dict:
        race_info = {}
        soup = get_soup(url)
        infos = soup.find("ul", {"class": "infolist"}).find_all("li")
        for i in range(len(infos) - 1):
            k, v = infos[i].text.split(":", 1)
            if v != "":
                race_info[string_fmt(k)] = string_fmt(v)

        return race_info

    def parse_one_day_race(self, race, race_name) -> dict:
        race_dict = {}
        race_url = f"https://www.procyclingstats.com/{self.find_race_url(str(race[4]))}"
        race_dict["result"] = race[1].text
        race_dict["name"] = string_fmt(race[4].text)
        race_dict["distance"] = race[5].text
        race_dict["is_stage"] = race_name.startswith("stage_")
        race_dict["race_url"] = race_url
        race_dict.update(self.scrape_race_info(race_url))
        return race_dict

    @staticmethod
    def is_race(race):
        """races do not have a date, thus we can use regex to check if date is 28.09, for example"""
        pattern = r"^\d{2}\.\d{2}$"
        return re.match(pattern, race[0].text) is not None


    def scrape_rider_year(self, year: str) -> pd.DataFrame:
        soup = get_soup(f"https://www.procyclingstats.com/rider/{self.name}/{year}")
        races = soup.find("tbody").find_all("tr")
        year_dict = {}
        for race in races:
            race = race.find_all("td")
            if self.is_race(race):
                race_name = string_fmt(race[4].text)
                year_dict[race_name] = self.parse_one_day_race(race, race_name)

        return pd.DataFrame.from_dict(year_dict, orient= 'index')


