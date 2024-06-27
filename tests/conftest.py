#!/usr/bin/env python3

import pytest


@pytest.fixture(scope="session")
def client():
    from gpt_racing import iracing_data

    client = iracing_data.IracingDataClient()

    return client


class FakeClient:
    def __init__(self):
        self._lap_data = {}
        self._qualy_data = {}

    def _set_lap_data(self, subsession_id, data):
        self._lap_data[subsession_id] = data

    def get_lap_data(self, subsession_id):
        return self._lap_data[subsession_id]

    def _set_qualy_result(self, subsession_id, data):
        self._qualy_data[subsession_id] = data

    def get_qualy_result(self, subsession_id):
        return self._qualy_data[subsession_id]


@pytest.fixture()
def fake_client():
    client = FakeClient()
    return client
