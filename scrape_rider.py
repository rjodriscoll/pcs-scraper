from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime as dt
import re
import datetime
import json


def string_fmt(s):
    return s.strip().replace(" ", "_").lower()


def title_fmt(s):
    return "_".join([s.lower() for s in s.split(" ") if s != ""])

def get_rider_homepage_stats(name: str):
    rider_dict = {}
    URL = f"https://www.procyclingstats.com/rider/{name}"
    page = requests.get(URL)
    p = page.text
    soup = BeautifulSoup(p, "lxml")
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

    points = soup.find_all("div", {"class": "pnt"})  # points
    ranks = soup.find_all("div", {"class": "rnk"})  # rankings
    titles_pnts = soup.find_all("div", {"class": "title"})[0 : len(points)]
    titles_ranks = soup.find_all("div", {"class": "title"})[
        len(points) : len(points) + len(ranks)
    ]

    for ind, title in enumerate(titles_pnts):
        rider_dict[string_fmt(title.text)] = int(points[ind].text)

    for ind, title in enumerate(titles_ranks):
        rider_dict[string_fmt(title.text)] = int(ranks[ind].text)

    rider_dict["date_scraped"] = datetime.datetime.now().strftime("%d/%m/%Y-%H:%M:%S")
    return rider_dict


def find_race_url(s):
    pattern = 'href="(.+?)"'
    match = re.search(pattern, s)
    if match:
        return match.group(1)


def scrape_race_info(url):
    race_info = {}
    page = requests.get(url)
    p = page.text
    soup = BeautifulSoup(p, "lxml")
    infos = soup.find("ul", {"class": "infolist"}).find_all("li")
    for i in range(len(infos) - 1):
        k, v = infos[i].text.split(":", 1)
        if v != "":
            race_info[string_fmt(k)] = string_fmt(v)

    return race_info


def parse_one_day_race(race, race_name):
    race_dict = {}
    race_url = f"https://www.procyclingstats.com/{find_race_url(str(race[4]))}"
    race_dict["result"] = race[1].text
    race_dict["name"] = string_fmt(race[4].text)
    race_dict["distance"] = race[5].text
    race_dict["is_stage"] = race_name.startswith("stage_")
    race_dict["race_url"] = race_url
    race_dict.update(scrape_race_info(race_url))
    return race_dict


def is_race(race):
    """races do not have a date, thus we can use regex to check if date is 28.09, for example"""
    pattern = r"^\d{2}\.\d{2}$"
    return re.match(pattern, race[0].text) is not None


def scrape_rider_year(name, year):
    url = f"https://www.procyclingstats.com/rider/{name}/{year}"
    page = requests.get(url)
    p = page.text
    soup = BeautifulSoup(p, "lxml")
    races = soup.find("tbody").find_all("tr")
    year_dict = {}
    for race in races:
        race = race.find_all("td")
        if is_race(race):
            race_name = string_fmt(race[4].text)
            year_dict[race_name] = parse_one_day_race(race, race_name)

    return year_dict


def get_stats_and_races(rider: str, years: list[str]):
    store = get_rider_homepage_stats(rider)

    races = {}
    for year in years:
        races[year] = scrape_rider_year(rider, year)

    store["results"] = races

    return store


def dump_to_json_file(data, rider_name):
    with open(f'{rider_name}.json', 'w') as outfile:
        json.dump(data, outfile)