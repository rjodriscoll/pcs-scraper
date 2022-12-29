import argparse
from tqdm import tqdm
from scrape import Scraper
from utils import get_team_riders

# Create an ArgumentParser object
parser = argparse.ArgumentParser()

# Add the rider and year arguments
parser.add_argument(
    "--riders", nargs="?", help="Name(s) of the rider(s) e.g. thomas-pidcock"
)
parser.add_argument(
    "--teams", nargs="?", help="Name(s) of the teams(s) e.g. uae-team-emirates-2022"
)
parser.add_argument("years", nargs="+", help="Year(s) to scrape")

# Parse the command-line arguments
args = parser.parse_args()

# Get the list of riders and years from the parsed arguments
years = args.years
riders = [args.riders] if args.riders else []
if args.teams:
    for team in args.teams:
        riders.extend(get_team_riders(team))

# Loop over the riders and years
with tqdm(total=len(riders) * len(years), desc="Scraping data") as pbar:
    for rider in riders:
        for year in years:
            print('Scraping data for rider: ' + rider)
            # Create a Scraper object
            s = Scraper(rider_url=f"https://www.procyclingstats.com/rider/{str(rider)}")

            # Scrape the homepage and save the data to a Parquet file
            s.scrape_homepage_to_parquet()

            # Scrape the data for the specified year and save it to a Parquet file
            s.scrape_year_to_parquet([str(year)])

            # Update the progress bar
            pbar.update(1)
