#!/usr/bin/env python3

import pytest


@pytest.fixture(scope="session")
def client():
    from gpt_racing import iracing_data

    client = iracing_data.IracingDataClient()

    return client
