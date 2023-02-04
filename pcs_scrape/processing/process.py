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

    @staticmethod
    def _parse_date(column: pd.Series) -> pd.Series:
        return pd.to_datetime(column, format="%d_%B_%Y")

    @staticmethod
    def _parse_distance(column: pd.Series) -> pd.Series:
        return pd.to_numeric(column.str.replace("_km", ""), errors ='coerce')

    @staticmethod
    def _parse_speed(column: pd.Series) -> pd.Series:
        return pd.to_numeric(column.str.replace("_km/h", ""), errors ='coerce')

    @staticmethod
    def _parse_stage_number(column: pd.Series) -> pd.Series:
        def _extract_stage(string):
            if string.startswith("stage"):

                stg = string.split("_")[1]
                return int("".join(filter(str.isdigit, stg)))
            else:
                return None

        return column.apply(_extract_stage)

    @staticmethod
    def _parse_result(column: pd.Series) -> pd.Series:
        return pd.to_numeric(
            column.replace({"DNF": np.nan, "DNS": np.nan, "DS": np.nan, "DF": np.nan}), downcast="integer", errors='coerce'
        )

    @staticmethod
    def _parse_start_time(column: pd.Series) -> pd.Series:
        def _parse_start_time(val):
            try:
                time = pd.to_datetime(val[0:5], format="%H:%M").hour
            except:
                time = 9
            return time

        return column.apply(_parse_start_time)

    @staticmethod
    def _make_cols_numeric(df: pd.DataFrame, columns: list[str]):
        for column in columns:
            df[column] = df[column].apply(pd.to_numeric)
        return df

    def _extract_date_time_to_features(self, df: pd.DataFrame):
        df["day"] = df["date"].dt.day
        df["month"] = df["date"].dt.month
        df["year"] = df["date"].dt.year
        df["hour"] = self._parse_start_time(df["start_time"])
        return df

    @staticmethod
    def _parse_winning_method(column: pd.Series) -> pd.Series:
        def _parse_method(event):
            if event:
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
            else:
                return None

        return column.apply(_parse_method)

    @staticmethod
    def _calculate_time_since_last_race(df: pd.DataFrame) -> pd.Series:
        df = df.sort_values("date", ascending=True)
        ser = df["date"] - df["date"].shift(1)
        return ser.dt.days

    @staticmethod
    def _calculate_racedays_this_year(df: pd.DataFrame) -> pd.Series:
        df = df.sort_values("date", ascending=True)
        data = df.copy()

        def _get_days(data):
            return len(df[(df.date < data.date) & (df.date.dt.year == data.date.year)])

        return data.apply(_get_days, axis=1)

    @staticmethod
    def _calculate_profilescore_vert_ratio(
        profile_col: pd.Series, vert_col: pd.Series
    ) -> pd.Series:

        return vert_col / profile_col

    @staticmethod
    def _calculate_best_result_this_year(df: pd.DataFrame) -> pd.Series:
        data = df.copy()

        def _get_best(data):
            return (
                df[(df.date < data.date) & (df.date.dt.year == data.date.year)][
                    "result"
                ]
                .apply(pd.to_numeric)
                .min()
            )

        return data.apply(_get_best, axis=1)

    @staticmethod
    def _calculate_performance_similar_races(df: pd.DataFrame) -> pd.Series:
        data = df.copy()

        def _get_similar_results(data):
            df_num = df[(df.date < data.date)].copy()

            if len(df_num) > 5:
                # if we have 5 data points we find the best results from the most similar races they've done.
                # this will get better as they've done more races
                cols = ["distance", "profilescore", "startlist_quality_score"]
                df_num["delta_sum"] = df_num.apply(
                    lambda row: euclidean(row[cols].values, data[cols].values), axis=1
                )
                df_num = df_num.sort_values("delta_sum", ascending=True)[1:6]
                return df_num.result.min()
            return np.nan

        return data.apply(_get_similar_results, axis=1)

    def _add_rider_details(self, df: pd.DataFrame) -> pd.DataFrame:
        stats = self.stats_df.reindex(
            self.stats_df.index.repeat(len(self.race_df))
        ).reset_index()
        joined = pd.concat([stats, df.reset_index()], axis=1)
        return joined

    def run_parsers(self):
        self.race_df["date"] = self._parse_date(self.race_df["date"])
        self.race_df["distance"] = self._parse_distance(self.race_df["distance"])
        self.race_df["avg._speed_winner"] = self._parse_speed(
            self.race_df["avg._speed_winner"]
        )
        self.race_df["stage_number"] = self._parse_stage_number(self.race_df["name"])
        self.race_df["result"] = self._parse_result(self.race_df["result"])
        self.race_df["won_how"] = self._parse_winning_method(self.race_df["won_how"])
        self.race_df = self._make_cols_numeric(
            self.race_df,
            [
                "result",
                "profilescore",
                "profilescore",
                "vert._meters",
                "startlist_quality_score",
            ],
        )

    def run_feature_extract(self):
        self.race_df = self._add_rider_details(self.race_df)
        self.race_df = self._extract_date_time_to_features(self.race_df)
        self.race_df["time_since_last_race"] = self._calculate_time_since_last_race(
            self.race_df
        )
        self.race_df["race_days_this_year"] = self._calculate_racedays_this_year(
            self.race_df
        )
        self.race_df[
            "profile_score_vert_ratio"
        ] = self._calculate_profilescore_vert_ratio(
            self.race_df["profilescore"], self.race_df["vert._meters"]
        )
        self.race_df["best_result_this_year"] = self._calculate_best_result_this_year(
            self.race_df
        )
        self.race_df[
            "best_result_similar_races"
        ] = self._calculate_performance_similar_races(self.race_df)

    def process_to_file(self):
        self.run_parsers()
        self.run_feature_extract()
        self.race_df.to_csv(self.rider_path + "processed_race_data.csv", index=False)
