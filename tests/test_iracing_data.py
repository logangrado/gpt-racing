#!/usr/bin/env python3


import pandas as pd

from gpt_racing import iracing_data


class TestGetRaceResults:
    def test_basic(self):
        client = iracing_data.IracingDataClient()

        subsession_id = 63788123
        result = client.get_race_result(subsession_id)

        subresult = result[["user_id", "display_name", "finish_position"]]
        pd.testing.assert_frame_equal(
            subresult,
            pd.DataFrame(
                [
                    {"user_id": 904207, "display_name": "Kacper Jargon", "finish_position": 0},
                    {"user_id": 428988, "display_name": "Jose Tacuri", "finish_position": 1},
                    {"user_id": 355935, "display_name": "Isitan Ibrahim", "finish_position": 2},
                    {"user_id": 274289, "display_name": "Anthony Downes", "finish_position": 3},
                    {"user_id": 575116, "display_name": "Trevor Mckeeman", "finish_position": 4},
                    {"user_id": 616834, "display_name": "Patrick Kaufmann", "finish_position": 5},
                    {"user_id": 335711, "display_name": "Ruud Schoorl", "finish_position": 6},
                    {"user_id": 219282, "display_name": "Giovani Diaz", "finish_position": 7},
                    {"user_id": 195952, "display_name": "Jose Cobos", "finish_position": 8},
                    {"user_id": 329266, "display_name": "Geson Fourny", "finish_position": 9},
                    {"user_id": 227658, "display_name": "Bruce Granheim", "finish_position": 10},
                    {"user_id": 140220, "display_name": "Johnathan Evans2", "finish_position": 11},
                    {"user_id": 360152, "display_name": "Rokas Stonys", "finish_position": 12},
                    {"user_id": 520127, "display_name": "Don Day", "finish_position": 13},
                    {"user_id": 762153, "display_name": "Terry Sloman", "finish_position": 14},
                    {"user_id": 694295, "display_name": "Tyler B Anthony", "finish_position": 15},
                    {"user_id": 416521, "display_name": "Denner Lima", "finish_position": 16},
                    {"user_id": 428734, "display_name": "Ruben Portillo", "finish_position": 17},
                    {"user_id": 622340, "display_name": "Logan Grado", "finish_position": 18},
                    {"user_id": 813686, "display_name": "Yery Ruiz", "finish_position": 19},
                ]
            ),
        )
