#!/usr/bin/env python3


import pandas as pd

from gpt_racing.iracing_data import IracingDataClient


def main():
    client = IracingDataClient(cache=False)

    result = client._client.get_tracks()

    track_df = pd.DataFrame(result)
    print(f"Total tracks: {len(track_df)}")

    # Filter for road
    track_df = track_df[track_df["category"] == "road"]
    print(f"Total road: {len(track_df)}")

    track_df = track_df[~track_df["track_name"].str.contains("\[Legacy\]|\[Retired\]")].reset_index(drop=True)

    print(f"Total current: {len(track_df)}")

    track_df = track_df[
        [
            "track_name",
            "config_name",
            "free_with_subscription",
            "fully_lit",
            "night_lighting",
            "nominal_lap_time",
        ]
    ]

    track_df = track_df.sort_values(
        by=["free_with_subscription", "track_name", "config_name"],
        ascending=[False, True, True],
    )

    print(track_df[["track_name", "free_with_subscription"]].drop_duplicates().value_counts("free_with_subscription"))

    track_df.to_csv("tracks.csv", index=False)
    import ipdb

    ipdb.set_trace()
    pass


if __name__ == "__main__":
    main()
