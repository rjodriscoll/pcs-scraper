import pandas as pd
import numpy as np


def parse_date(column: pd.Series) -> pd.Series:
    return pd.to_datetime(column, format="%d_%B_%Y")


def parse_distance(column: pd.Series) -> pd.Series:
    return pd.to_numeric(column.str.replace("_km", ""))


def parse_speed(column: pd.Series) -> pd.Series:
    return pd.to_numeric(column.str.replace("_km/h", ""))


def parse_stage_number(column: pd.Series) -> pd.Series:
    def _extract_stage(string):
        if string.startswith("stage"):
            return int(string.split("_")[1])
        else:
            return None

    return column.apply(_extract_stage)


def replace_dnf_dns(column: pd.Series) -> pd.Series:
    return column.replace({"DNF": np.nan, "DNS": np.nan})


def parse_winning_method(column: pd.Series) -> pd.Series:
    def _parse_method(event):
        if "solo" in event:
            return "solo"
        elif "small_group" in event:
            return "small_sprint"
        elif "large_group" in event:
            return "large_sprint"
        elif "sprint_a_deux" in event:
            return "sprint_a_deux"
        elif "time_trial" in event:
            return "time_trial"
        else:
            return event

    return column.apply(_parse_method)


def calculate_time_since_last_race(df: pd.DataFrame) -> pd.Series:
    df = df.sort_values("date", ascending=True)
    return df["date"] - df["date"].shift(1)


def calculate_racedays_this_year(df: pd.DataFrame) -> pd.Series:
    df = df.sort_values("date", ascending=True)
    data = df.copy()

    def _get_days(data):
        return len(df[(df.date < data.date) & (df.date.dt.year == data.date.dt.year)])

    return data.apply(_get_days, axis=1)


def calculate_profilescore_vert_ratio(
    profile_col: pd.Series, vert_col: pd.Series
) -> pd.Series:
    return vert_col / profile_col


def calculate_best_result_this_year(df: pd.DataFrame) -> pd.Series:
    df["result"] = replace_dnf_dns(df["result"])
    data = df.copy()

    def _get_best(data):
        return (
            df[(df.date < data.date) & (df.date.dt.year == data.date.dt.year)]["result"]
            .apply(pd.to_numeric)
            .min()
        )

    return data.apply(_get_best, axis=1)


def calculate_performance_similar_profile():
    pass


def add_rider_details():
    pass
