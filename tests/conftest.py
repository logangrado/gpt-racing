#!/usr/bin/env python3

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--update-refs",
        action="store_true",
    )


@pytest.fixture(scope="session")
def client():
    from gpt_racing import iracing_data

    client = iracing_data.IracingDataClient()

    return client


class FakeClient:
    def __init__(self):
        self._lap_data = {}
        self._qualy_data = {}
        self._race_data = {}

    def _set_lap_data(self, subsession_id, data):
        self._lap_data[subsession_id] = data

    def get_lap_data(self, subsession_id):
        return self._lap_data[subsession_id]

    def _set_qualy_result(self, subsession_id, data):
        self._qualy_data[subsession_id] = data

    def get_qualy_result(self, subsession_id):
        return self._qualy_data[subsession_id]

    def _set_race_result(self, subsession_id, data):
        self._race_data[subsession_id] = data

    def get_race_result(self, subsession_id):
        """

        Returns one row per driver, in the following format:

        cust_id                                          346566
        display_name                            Derek M Cyphers
        finish_position                                       0
        finish_position_in_class                              0
        interval                                              0
        average_lap                                      896126
        laps_complete                                        41
        class_interval                                        0
        qual_lap_time                                        -1
        starting_position                                     0
        starting_position_in_class                            0
        incidents                                             0
        car_class_name                    Hosted All Cars Class
        reason_out                                      Running
        subsession_id                                  66010063
        session_end_time              2024-01-09 02:34:35+00:00

        dtypes:

        cust_id                                    int64
        display_name                              object
        finish_position                            int64
        finish_position_in_class                   int64
        interval                                   int64
        average_lap                                int64
        laps_complete                              int64
        class_interval                             int64
        qual_lap_time                              int64
        starting_position                          int64
        starting_position_in_class                 int64
        incidents                                  int64
        car_class_name                            object
        reason_out                                object
        subsession_id                              int64
        session_end_time              datetime64[s, UTC]

        """
        return self._race_data[subsession_id]


@pytest.fixture()
def fake_client():
    client = FakeClient()
    return client
