from bs4 import BeautifulSoup
import requests
import re
import http


def string_fmt(s):
    return s.strip().replace(" ", "_").lower()


def title_fmt(s):
    return "_".join([s.lower() for s in s.split(" ") if s != ""])


def get_soup(URL):
    r = requests.get(URL)
    if (r.status_code != http.HTTPStatus.OK) and (
        r.status_code != http.HTTPStatus.FOUND
    ):
        raise RuntimeError(f"Error retrieving from {URL}, code:", r.status_code)
    return BeautifulSoup(r.text, "lxml")


def get_team_riders(team: str) -> list[str]:
    """Get list of riders for a team

    Args:
        team (str): e.g. uae-team-emirates-2022

    Returns:
        list[str]: list of riders
    """
    soup = get_soup(f"https://www.procyclingstats.com/team/{team}/overview")
    riders = soup.find("span", class_="table-cont").find_all(
        "a", href=re.compile(r"^rider/.*")
    )
    return [rider["href"].split("/")[-1] for rider in riders]
