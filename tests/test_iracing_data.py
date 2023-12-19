#!/usr/bin/env python3


import pandas as pd
import pytest

from gpt_racing import iracing_data


class TestCachedIRClient:
    def test_result(self):
        client = iracing_data.CachedIRClient()

        x = client.result(63788123)
        import ipdb

        ipdb.set_trace()
        pass


class TestRaceResults:
    def test_race_result(self, client):
        subsession_id = 65426723
        result = client.get_race_result(subsession_id)

        subresult = result[["user_id", "display_name", "finish_position", "average_lap", "starting_position"]]

        pd.testing.assert_frame_equal(
            subresult,
            pd.DataFrame(
                [
                    {
                        "user_id": 407068,
                        "display_name": "Christian Youngwall",
                        "finish_position": 0,
                        "average_lap": 901658,
                        "starting_position": 1,
                    },
                    {
                        "user_id": 483350,
                        "display_name": "Reed Gibson",
                        "finish_position": 1,
                        "average_lap": 905043,
                        "starting_position": 2,
                    },
                    {
                        "user_id": 412321,
                        "display_name": "John Sedlak",
                        "finish_position": 2,
                        "average_lap": 905379,
                        "starting_position": 10,
                    },
                    {
                        "user_id": 260862,
                        "display_name": "Rafael Amorim",
                        "finish_position": 3,
                        "average_lap": 905854,
                        "starting_position": 5,
                    },
                    {
                        "user_id": 586629,
                        "display_name": "Enrico Gregoratto",
                        "finish_position": 4,
                        "average_lap": 908030,
                        "starting_position": 4,
                    },
                    {
                        "user_id": 663448,
                        "display_name": "Scott Kessel",
                        "finish_position": 5,
                        "average_lap": 912496,
                        "starting_position": 8,
                    },
                    {
                        "user_id": 346566,
                        "display_name": "Derek M Cyphers",
                        "finish_position": 6,
                        "average_lap": 912708,
                        "starting_position": 6,
                    },
                    {
                        "user_id": 282802,
                        "display_name": "Ross Yost",
                        "finish_position": 7,
                        "average_lap": 913108,
                        "starting_position": 3,
                    },
                    {
                        "user_id": 414448,
                        "display_name": "Justin SW Tsang",
                        "finish_position": 8,
                        "average_lap": 914100,
                        "starting_position": 19,
                    },
                    {
                        "user_id": 538346,
                        "display_name": "Ariel Julian",
                        "finish_position": 9,
                        "average_lap": 919437,
                        "starting_position": 11,
                    },
                    {
                        "user_id": 577153,
                        "display_name": "Matt Scherff",
                        "finish_position": 10,
                        "average_lap": 921092,
                        "starting_position": 13,
                    },
                    {
                        "user_id": 622340,
                        "display_name": "Logan Grado",
                        "finish_position": 11,
                        "average_lap": 921238,
                        "starting_position": 18,
                    },
                    {
                        "user_id": 696009,
                        "display_name": "Jochem Bakker",
                        "finish_position": 12,
                        "average_lap": 924247,
                        "starting_position": 16,
                    },
                    {
                        "user_id": 686056,
                        "display_name": "Chad Henson",
                        "finish_position": 13,
                        "average_lap": 929698,
                        "starting_position": 7,
                    },
                    {
                        "user_id": 367874,
                        "display_name": "Matthew Siddall",
                        "finish_position": 14,
                        "average_lap": 930079,
                        "starting_position": 12,
                    },
                    {
                        "user_id": 384356,
                        "display_name": "Chris Walsh",
                        "finish_position": 15,
                        "average_lap": 999534,
                        "starting_position": 9,
                    },
                    {
                        "user_id": 759324,
                        "display_name": "Evan Smogor",
                        "finish_position": 16,
                        "average_lap": 933764,
                        "starting_position": 14,
                    },
                    {
                        "user_id": 719884,
                        "display_name": "Robby Prescott",
                        "finish_position": 17,
                        "average_lap": 1352371,
                        "starting_position": 20,
                    },
                    {
                        "user_id": 405581,
                        "display_name": "Rodney Campbell2",
                        "finish_position": 18,
                        "average_lap": 0,
                        "starting_position": 15,
                    },
                    {
                        "user_id": 154321,
                        "display_name": "Leif Peterson",
                        "finish_position": 19,
                        "average_lap": 0,
                        "starting_position": 0,
                    },
                    {
                        "user_id": 464635,
                        "display_name": "Ed Eijsenring",
                        "finish_position": 20,
                        "average_lap": 0,
                        "starting_position": 17,
                    },
                    {
                        "user_id": 144629,
                        "display_name": "Michael Johnson21",
                        "finish_position": 21,
                        "average_lap": 0,
                        "starting_position": 21,
                    },
                    {
                        "user_id": 286691,
                        "display_name": "Jesse Lyon2",
                        "finish_position": 22,
                        "average_lap": 0,
                        "starting_position": 22,
                    },
                ]
            ),
        )

    def test_qualy_result(self, client):
        subsession_id = 65426723
        result = client.get_qualy_result(subsession_id)

        assert set(result.columns) == {
            "user_id",
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
            "subsession_id",
            "session_end_time",
        }

        assert len(result) == 23
        pd.testing.assert_frame_equal(
            result.iloc[:2],
            pd.DataFrame(
                [
                    {
                        "user_id": 260862,
                        "display_name": "Rafael Amorim",
                        "finish_position": 0,
                        "finish_position_in_class": 0,
                        "interval": 0,
                        "average_lap": 887449,
                        "laps_complete": 2,
                        "class_interval": 0,
                        "qual_lap_time": -1,
                        "starting_position": -1,
                        "starting_position_in_class": -1,
                        "incidents": 0,
                        "car_class_name": "Hosted All Cars Class",
                        "reason_out": "Running",
                        "subsession_id": 65426723,
                        "session_end_time": Pd.Timestamp("2023-12-12 02:16:29+0000", tz="UTC"),
                    },
                    {
                        "user_id": 154321,
                        "display_name": "Leif Peterson",
                        "finish_position": 1,
                        "finish_position_in_class": 1,
                        "interval": 1597,
                        "average_lap": 888409,
                        "laps_complete": 2,
                        "class_interval": 1597,
                        "qual_lap_time": -1,
                        "starting_position": -1,
                        "starting_position_in_class": -1,
                        "incidents": 0,
                        "car_class_name": "Hosted All Cars Class",
                        "reason_out": "Running",
                        "subsession_id": 65426723,
                        "session_end_time": Pd.Timestamp("2023-12-12 02:16:29+0000", tz="UTC"),
                    },
                ]
            ),
        )


@pytest.mark.skip(reason="Not implemented")
class TestGetLapData:
    def test_basic(self):
        pass


@pytest.mark.skip(reason="Not implemented")
class TestSearch:
    pass
