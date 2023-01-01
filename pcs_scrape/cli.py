import argparse
from tqdm import tqdm
from scrape import Scraper
from utils import get_team_riders
import warnings

parser = argparse.ArgumentParser()

parser.add_argument(
    "--riders", nargs="?", help="Name(s) of the rider(s) e.g. thomas-pidcock"
)
parser.add_argument(
    "--teams", nargs="?", help="Name(s) of the teams(s) e.g. uae-team-emirates-2022"
)

parser.add_argument("years", nargs="+", help="Year(s) to scrape")

args = parser.parse_args()

years = args.years
riders = [args.riders] if args.riders else []
output_fmt = 'parquet' if not args.output_format else args.output_format
if args.teams:
    for team in args.teams:
        riders.extend(get_team_riders(team))

with tqdm(total=len(riders) * len(years), desc="Scraping data") as pbar:
    for rider in riders:
        for year in years:
            print("Scraping data for rider: " + rider)

            s = Scraper(rider_url=f"https://www.procyclingstats.com/rider/{str(rider)}")
            try:
                s.scrape_homepage()
            except RuntimeError as e:
                warnings.warn(
                    f"could not retrieve data from {s.url}, the following status code was returned: {str(e.args[1])}"
                )
            try:
                s.scrape_year([str(year)])
            except RuntimeError as e:
                warnings.warn(
                    f"could not retrieve data for {str(rider)} {str(year)}, the following status code was returned: {str(e.args[1])}"
                )
            pbar.update(1)
