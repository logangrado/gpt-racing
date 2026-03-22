#!/usr/bin/env python3

import getpass
import hashlib
import inspect
import json
import os
import warnings
from datetime import datetime

import polars as pl
from iracing_oauth_client import Config, IRacingOAuthClient
from iracingdataapi.client import irDataClient

from gpt_racing import CACHE_PATH, logger
from gpt_racing.vault import vault


def _get_iracing_oauth_client():

    config = Config()

    # Create OAuth client
    client = IRacingOAuthClient(
        client_id=os.environ.get("CLIENT_ID") or vault["client_id"],
        client_secret=os.environ.get("CLIENT_SECRET") or vault["client_secret"],
        username=os.environ.get("CLIENT_USERNAME") or vault["username"],
        password=os.environ.get("CLIENT_PASSWORD") or vault["password"],
        request_timeout=config.request_timeout,
        token_refresh_buffer_seconds=config.token_refresh_buffer_seconds,
        log_level=config.log_level,
        log_format=config.log_format,
        ir_env=config.ir_env,
    )

    # Authenticate
    if client.authenticate():
        print("Successfully authenticated!")
        print(f"Token expires at: {client.token_expires_at}")
    else:
        print("Authentication failed!")

    return client


def _get_token() -> str:
    oauth_client = _get_iracing_oauth_client()

    token = str(oauth_client.access_token)

    return token


def _load_client() -> irDataClient:
    """
    Retrieve an IRDataClient.

    Asks for username/password, if not already stored. Password will be stored encrypted.

    Else, loads client using existing username/password
    """

    # use_pydantic=True is the library's desired path but iracingdataapi's own
    # MemberProfileResponse model incorrectly requires logo_url: str when the
    # API can return None — causing ValidationError against real data. Suppress
    # the deprecation until the library fixes its model.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        ir_client = irDataClient(access_token=_get_token())

    return ir_client


def _get_race_result(result: dict) -> pl.DataFrame:
    """
    Get the race result from a result dict
    """
    for session in result["session_results"][::-1]:
        if session["simsession_type_name"].upper() == "RACE":
            df_all = pl.DataFrame(session["results"])

            # Filter out AIs
            df_all = df_all.filter(pl.col("ai") == False)

            # Select columns we care about
            df = df_all.select(
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
            )

            df = df.with_columns(
                pl.lit(result["subsession_id"]).alias("subsession_id"),
                pl.lit(datetime.fromisoformat(result["end_time"])).alias("session_end_time"),
            )
            return df

    return None


def _get_qualy_result(result: dict) -> pl.DataFrame:
    """
    Get the race result from a result dict
    """
    for session in result["session_results"][::-1]:
        if "QUALIFY" in session["simsession_type_name"].upper():
            df_all = pl.DataFrame(session["results"])

            # Filter out AIs
            df_all = df_all.filter(pl.col("ai") == False)

            # Select columns we care about
            df = df_all.select(
                [
                    "cust_id",
                    "display_name",
                    "finish_position",
                    "finish_position_in_class",
                    "average_lap",
                    "best_lap_time",
                    "interval",
                    "laps_complete",
                    "class_interval",
                    "qual_lap_time",
                    "incidents",
                    "reason_out",
                ]
            )

            df = df.with_columns(
                pl.lit(result["subsession_id"]).alias("subsession_id"),
                pl.lit(datetime.fromisoformat(result["end_time"])).alias("session_end_time"),
            )
            return df

    raise ValueError("")


def _collect_func_signature_and_args(func: callable, args: tuple, kwargs: dict) -> dict:
    """
    Inspects the given function `func`, and determines all positional and keyword arguments that the function has.
    It then combines these with the provided `args` and `kwargs`.

    Parameters:
    - func (callable): The function to inspect.
    - args (tuple): The positional arguments provided to the function.
    - kwargs (dict): The keyword arguments provided to the function.

    Returns:
    - dict: A dictionary containing all arguments and their values.
    """
    # Get the signature of the function
    sig = inspect.signature(func)

    # Initialize a dictionary to hold the arguments and their default values
    args_dict = {**kwargs}

    # Iterate over the parameters of the function
    for i, (name, param) in enumerate(sig.parameters.items()):
        # Check if we have a provided positional arg
        if len(args) > i:
            if name in args_dict:
                raise ValueError(f"Multiple values provided for arg {name}")
            args_dict[name] = args[i]

        else:
            if name not in args_dict:
                args_dict[name] = param.default

    return args_dict


class CachedIRClient:
    def __init__(self, cache_path):
        self._client = None  # authenticated lazily on first cache miss
        self._cache_path = cache_path

    def _get_client(self):
        if self._client is None:
            self._client = _load_client()
        return self._client

    def _cache_wrapper(self, func, func_name, z=42):
        def wrapper(*args, **kwargs):
            # Collect all args
            all_kwargs = _collect_func_signature_and_args(func, args, kwargs)

            # Determine cache path
            hash_str = hashlib.sha256((func_name + json.dumps(all_kwargs, sort_keys=True)).encode()).hexdigest()
            cache_path = self._cache_path / "ir_data_client" / hash_str[:2] / hash_str[2:]
            data_path = cache_path / "data.json"

            # Check for cache.
            if data_path.is_file():
                logger.info("Reading cache")
                with open(data_path, "r") as f:
                    result = json.load(f)
            else:
                # Cache miss — authenticate and fetch
                logger.info("Retrieving result")
                result = func(*args, **kwargs)
                data_path.parent.mkdir(exist_ok=True, parents=True)
                logger.info("Writing cache")
                with open(data_path, "w") as f:
                    json.dump(result, f)

            return result

        return wrapper

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return getattr(self, attr)

        return self._cache_wrapper(getattr(self._get_client(), attr), attr)


class IracingDataClient:
    def __init__(self, cache=True, cache_path=CACHE_PATH):
        if cache:
            self._client = CachedIRClient(cache_path=cache_path)
        else:
            self._client = _load_client()

        self._cust_id = self._client.member_profile()["cust_id"]

    def get_race_result(self, subsession_id: int) -> pl.DataFrame:
        result = self._client.result(subsession_id)

        # Find the race results
        session_result = result["session_results"]

        race_result = _get_race_result(result)

        return race_result

    def get_qualy_result(self, subsession_id: int) -> pl.DataFrame:
        result = self._client.result(subsession_id)

        # Find the race results
        session_result = result["session_results"]

        race_result = _get_qualy_result(result)

        return race_result

    def get_lap_data(self, subsession_id: int) -> pl.DataFrame:
        return pl.DataFrame(self._client.result_lap_chart_data(subsession_id))

    def search_sessions(self, start_time, end_time, cust_id=None):
        if cust_id is None:
            cust_id = self._cust_id

        # Start_time/end_time must be no more than 90 days apart!

        return pl.DataFrame(
            self._client.result_search_hosted(
                start_range_begin=start_time,
                start_range_end=end_time,
                cust_id=cust_id,
            )
        )
