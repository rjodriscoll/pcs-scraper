import pandas as pd
import numpy as np
from scipy.spatial.distance import euclidean
import os
import re



class Processor:
    def __init__(self, rider_path: str):
        self.rider_path = rider_path
        self.stats_df = pd.read_parquet(f"{rider_path}/stats.parquet")
        self.race_df = pd.concat(
            [pd.read_parquet(self.rider_path + file) for file in self._get_race_files()]
        )
        self.stage_df = pd.concat(
            [
                pd.read_parquet(self.rider_path + file)
                for file in self._find_all_stage_races()
            ]
        )

    def _get_race_files(self):
        pattern = r"\d{4}_races\.parquet$"
        return [
            file for file in os.listdir(self.rider_path) if re.search(pattern, file)
        ]

    def _find_all_stage_races(self):
        return [
            file
            for file in os.listdir(self.rider_path)
            if file.endswith("stage_races.parquet")
        ]

    def _parse_date(column: pd.Series) -> pd.Series:
        return pd.to_datetime(column, format="%d_%B_%Y")

    def _parse_distance(column: pd.Series) -> pd.Series:
        return pd.to_numeric(column.str.replace("_km", ""))

    def _parse_speed(column: pd.Series) -> pd.Series:
        return pd.to_numeric(column.str.replace("_km/h", ""))

    def _parse_stage_number(column: pd.Series) -> pd.Series:
        def _extract_stage(string):
            if string.startswith("stage"):
                return int(string.split("_")[1])
            else:
                return None

        return column.apply(_extract_stage)

    def _parse_result(column: pd.Series) -> pd.Series:
        return pd.numeric(column.replace({"DNF": np.nan, "DNS": np.nan}))

    def _parse_start_time(column: pd.Series) -> pd.Series:
        def _parse_start_time(val):
            time = pd.to_datetime(val[0:5], format="%Y-%m-%d %H:%M")
            return time.ti

        return column.apply(_parse_start_time)

    def _make_cols_numeric(df: pd.DataFrame, columns: list[str]):
        for column in columns:
            df[column] = df[column].apply(pd.to_numeric)
        return df

    def _extract_date_time_to_features(df: pd.DataFrame):
        df["day"] = df["date"].dt.day
        df["month"] = df["date"].dt.month
        df["year"] = df["date"].dt.year
        df["hour"] = df["start_time"].dt.hour
        return df

    def _parse_winning_method(column: pd.Series) -> pd.Series:
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

    def _calculate_time_since_last_race(df: pd.DataFrame) -> pd.Series:
        df = df.sort_values("date", ascending=True)
        return df["date"] - df["date"].shift(1)

    def _calculate_racedays_this_year(df: pd.DataFrame) -> pd.Series:
        df = df.sort_values("date", ascending=True)
        data = df.copy()

        def _get_days(data):
            return len(
                df[(df.date < data.date) & (df.date.dt.year == data.date.dt.year)]
            )

        return data.apply(_get_days, axis=1)

    def _calculate_profilescore_vert_ratio(
        profile_col: pd.Series, vert_col: pd.Series
    ) -> pd.Series:
        return vert_col / profile_col

    def _calculate_best_result_this_year(self, df: pd.DataFrame) -> pd.Series:
        df["result"] = self._replace_dnf_dns(df["result"])
        data = df.copy()

        def _get_best(data):
            return (
                df[(df.date < data.date) & (df.date.dt.year == data.date.dt.year)][
                    "result"
                ]
                .apply(pd.to_numeric)
                .min()
            )

        return data.apply(_get_best, axis=1)

    def _calculate_performance_similar_races(df):
        data = df.copy()

        def _get_similar_results(data):
            df_num = df[(df.date < data.date)].copy()

            if len(df_num) > 5:
                # if we have 5 data points we find the best results from the most similar races they've done.
                # this will get better as they've done more races
                df_num["delta_sum"] = df_num.apply(
                    lambda row: euclidean(row, data), axis=1
                )
                df_num = df_num.sort_values("delta_sum", ascending=True)[1:6]
                return df_num.result.min()
            return np.nan

        return data.apply(_get_similar_results, axis=1)

    def _add_rider_details(self, df: pd.DataFrame):
        stats = self.stats_df.reindex(
            self.stats_df.index.repeat(len(self.race_df))
        ).reset_index()
        joined = pd.concat([stats, df.reset_index()], axis=1)
        return joined

    def run_parsers(self):
        self.race_df["date"] = self._parse_date(self.race_df["date"])
        self.race_df["distance"] = self._parse_date(self.race_df["distance"])
        self.race_df["avg._speed_winner"] = self._parse_date(
            self.race_df["avg._speed_winner"]
        )
        self.race_df["stage_number"] = self._parse_stage_number(self.race_df["name"])
        self.race_df["start_time"] = self._parse_start_time(self.race_df["start_time"])
        self.race_df["result"] = self._parse_result(self.race_df["result"])
        self.race_df["won_how"] = self._parse_winning_method(self.race_df["won_how"])
        self.race_df = self._make_cols_numeric(
            self.race_df, ["result", "profilescore", "profilescore", "vert._meters"]
        )

    def run_feature_extract(self):
        self.race_df = self._add_rider_details(self.race_df)
        self.race_df = self._extract_date_time_to_features(self.race_df)
        self.race_df = self._calculate_time_since_last_race(self.race_df)
        self.race_df = self._calculate_racedays_this_year(self.race_df)
        self.race_df = self._calculate_profilescore_vert_ratio(self.race_df)
        self.race_df = self._calculate_best_result_this_year(self.race_df)
        self.race_df = self._calculate_performance_similar_races(self.race_df)