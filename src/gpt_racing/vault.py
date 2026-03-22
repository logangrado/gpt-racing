#!/usr/bin/env python3

import os
from pathlib import Path

from ansible_vault import Vault
from dotenv import load_dotenv


def get_vault() -> dict:
    load_dotenv()  # loads .env from CWD (no-op if absent)

    # Vault file: env var → cwd/vault.yml if present → skip
    vault_path_env = os.environ.get("VAULT_PATH")
    if vault_path_env:
        vault_path = Path(vault_path_env)
    else:
        vault_path = Path.cwd() / "vault.yml"

    if not vault_path.exists():
        return {}

    # Password file: env var → default location → skip
    pw_file_env = os.environ.get("VAULT_PASSWORD_FILE")
    if pw_file_env:
        pw_file = Path(pw_file_env)
    else:
        pw_file = Path.home() / ".ansible/passwords/gpt_racing.txt"

    if not pw_file.exists():
        return {}

    pw = pw_file.read_text().splitlines()[0].strip()
    vault = Vault(pw)
    return vault.load(vault_path.read_text())


vault: dict = get_vault()
