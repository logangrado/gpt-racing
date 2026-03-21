# gpt-racing

A racing analytics and league management tool for [iRacing](https://www.iracing.com/). Fetches race data from the iRacing API, computes championship standings and ELO/MMR driver ratings, and generates HTML reports and CSV exports.

## Features

- **Points standings** — configurable per-position points, drop weeks, fastest lap and cleanest driver bonuses, multiple points systems (e.g. major vs. normal events)
- **ELO/MMR ratings** — skill-based ratings that evolve across races, with support for carrying ratings across seasons
- **HTML reports** — formatted race result, standings, and ratings tables
- **CSV exports** — for further analysis
- **Penalty support** — apply time penalties to any driver in any race
- **Caching** — iRacing API responses cached locally by content hash

## Installation

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

### Credentials

iRacing credentials are stored in an Ansible vault at `vault.yml`. To set up:

```bash
ansible-vault edit vault.yml
```

The vault should contain:

```yaml
client_id: ...
client_secret: ...
username: ...
password: ...
```

Alternatively, set `CLIENT_ID`, `CLIENT_SECRET`, `CLIENT_USERNAME`, and `CLIENT_PASSWORD` as environment variables — these take precedence over the vault.

## Usage

```bash
uv run gpt-racing <config_path> <output_path>
```

- `config_path` — path to a [jsonnet](https://jsonnet.org/) config file
- `output_path` — directory where HTML and CSV outputs are written

## Configuration

Configs are written in jsonnet. A minimal example:

```jsonnet
{
  points: {
    drop_races: 1,
    points: [25, 18, 15, 12, 10, 8, 6, 4, 2, 1],
  },
  elo: {},
  races: [
    {
      subsession_id: 66010063,
    },
    {
      subsession_id: 66718020,
      race_name: "Interlagos",
      penalties: [
        { user_id: 161240, time: 5 },
      ],
    },
  ],
}
```

### Config reference

| Field | Type | Description |
|-------|------|-------------|
| `races[].subsession_id` | int | iRacing subsession ID |
| `races[].race_name` | str | Display name for the race |
| `races[].penalties[].user_id` | int | iRacing customer ID |
| `races[].penalties[].time` | float | Penalty in seconds |
| `races[].points_type` | str | Which points system to use (if multiple defined) |
| `points.points` | list or dict | Points per finishing position |
| `points.drop_races` | int | Number of worst results to drop per driver |
| `points.fastest_lap.points` | int | Bonus points for fastest lap |
| `points.cleanest_driver.points` | int | Bonus points for fewest incidents |
| `elo.min_races` | int | Minimum races required to appear in ratings |
| `elo.time_window` | str | Rolling window for ratings (e.g. `"365 days"`) |
| `elo.previous_seasons` | list | Jsonnet imports of prior season configs for rating carryover |

## Output

```
output_path/
├── csv/
│   ├── standings_race_1.csv
│   ├── race_results_race_1.csv
│   ├── ELO_race_1.csv
│   └── ...
└── pdf/
    ├── standings_race_1.html
    ├── results_race_1.html
    ├── elo_race_1.html
    └── ...
```

## Development

```bash
uv run invoke format          # format code
uv run invoke format --check  # check formatting without modifying
uv run pytest tests           # run tests
```
