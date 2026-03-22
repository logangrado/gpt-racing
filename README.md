# gpt-racing

<!-- [![PyPI](https://img.shields.io/pypi/v/seekr-hatchery)](https://pypi.org/project/seekr-hatchery/) -->
<!-- [![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://pypi.org/project/seekr-hatchery/) -->
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/logangrado/gpt-racing/badge)](https://securityscorecards.dev/viewer/?uri=github.com/logangrado/gpt-racing)


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

Credentials can be supplied in three ways (listed in precedence order):

**1. Environment variables** — set these in your shell or a `.env` file in your working directory:

```
CLIENT_ID=...
CLIENT_SECRET=...
CLIENT_USERNAME=...
CLIENT_PASSWORD=...
```

A `.env` file is loaded automatically when the CLI starts.

**2. Ansible vault** — store credentials encrypted in `vault.yml` in your working directory:

```bash
ansible-vault create vault.yml
```

The vault should contain:

```yaml
client_id: ...
client_secret: ...
username: ...
password: ...
```

The vault password is read from `~/.ansible/passwords/gpt_racing.txt` by default. Override with `VAULT_PASSWORD_FILE=/path/to/file`.

To use a vault file at a non-default path, set `VAULT_PATH=/path/to/vault.yml`.

**3. Running from source** — same options apply; the working directory is the repo root.

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

See `config.py`

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
