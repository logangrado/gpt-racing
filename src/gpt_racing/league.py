from gpt_racing.iracing_data import IracingDataClient


def fetch_new_races(
    client: IracingDataClient,
    league_id: int,
    season_id: int,
    existing_ids: set[int],
) -> list[dict]:
    """Returns completed sessions from the league season not already in existing_ids."""
    sessions = client.get_league_sessions(league_id, season_id)
    return [s for s in sessions if s["subsession_id"] not in existing_ids]
