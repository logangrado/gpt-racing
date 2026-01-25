#!/usr/bin/env python3

from pathlib import Path

import yaml
from ansible_vault import Vault

from gpt_racing import ROOT

VAULT_PATH = ROOT.parent.parent / "vault.yml"


def get_vault() -> dict:
    ansible_vault_pw_file = Path.home() / ".ansible/passwords/gpt_racing.txt"
    with open(ansible_vault_pw_file, "r") as f:
        pw = f.readlines()[0].strip()

    vault = Vault(pw)
    data = vault.load(open(VAULT_PATH).read())

    return data


vault: dict = get_vault()
