#!/usr/bin/env python3

import getpass
import json

from iracingdataapi.client import irDataClient

import pandas as pd

from gpt_racing import SECRETS_PATH

# idc = irDataClient(username=[YOUR iRACING USERNAME], password=[YOUR iRACING PASSWORD])

# # get the summary data of a member
# idc.get_member_summary(cust_id=20979)

# # get latest results of a member
# idc.get_member_recent_races(cust_id=209179)

# # get all laps for a specific driver in a race
# idc.get_result_lap_data(subsession_id=43720351, cust_id=209179)


def _load_secrets(path):
    with open(path, "r") as f:
        return json.load(f)


def _load_client() -> irDataClient:
    """
    Retrieve an IRDataClient.

    Asks for username/password, if not already stored. Password will be stored encrypted.

    Else, loads client using existing username/password
    """
    if SECRETS_PATH.exists():
        print(f"Loading credentials from {SECRETS_PATH}")
        with open(SECRETS_PATH, "r") as f:
            secrets = json.load(f)

        client = irDataClient(username=secrets["username"], password="")
        client.encoded_password = secrets["encoded_password"]

    else:
        print("Enter your iRacing username/password.")
        print(f"Your password will be stored encrypted in {SECRETS_PATH}")
        username = input("Username: ")
        password = getpass.getpass("Password: ")

        client = irDataClient(username=username, password=password)

        client.member_info()

        # get the encoded password
        data = {"username": username, "encoded_password": client.encoded_password}

        with open(SECRETS_PATH, "w") as f:
            json.dump(data, f)

    print(f"Authenticated with account '{client.username}'")
    return client


def _get_race_result(result: dict) -> pd.DataFrame:
    """
    Get the race result from a result dict
    """
    for session in result["session_results"]:
        if session["simsession_type_name"].upper() == "RACE":
            df_all = pd.DataFrame(session["results"])

            # Filter out AIs
            df_all = df_all[df_all["ai"] == False]

            # Select columns we care about
            df = df_all[
                [
                    "cust_id",
                    "display_name",
                    "finish_position",
                    "finish_position_in_class",
                    "interval",
                    "average_lap",
                    "laps_complete",
                    "class_interval",
                    "qual_lap_time",
                    "starting_position",
                    "starting_position_in_class",
                    "incidents",
                    "car_class_name",
                    "reason_out",
                ]
            ]

            # # Compute total time
            # df["total_time"] = df["average_lap"] * df["laps_complete"]
            return df

    return None


class IracingDataClient:
    def __init__(self):
        self._client = _load_client()
        # auth = _load_secrets(SECRETS_PATH)

        # self._client = irDataClient(**auth)
        self._cust_id = self._client.member_profile()["cust_id"]

    def get_race_result(self, subsession_id: int) -> pd.DataFrame:
        result = self._client.result(subsession_id)

        # Find the race results
        session_result = result["session_results"]

        race_result = _get_race_result(result)

        return race_result
