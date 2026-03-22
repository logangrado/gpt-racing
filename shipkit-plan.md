# Shipkit: GitHub Composite Action for Automated Releases

## Goal

Create a reusable GitHub composite action (`org/shipkit`) that bundles the full release pipeline into a single step: compute version, generate changelog, create git tag, and publish a GitHub release. Modelled on an existing internal GitLab `shipkit` implementation (shell scripts).

## Action type

**Composite action.** The existing implementation is shell scripts, so no runtime rewrite is needed. Composite actions run in the caller's workspace (giving access to the repo's git history, `pyproject.toml`, etc.) and reference scripts via `${{ github.action_path }}`.

## Repository structure

```
org/shipkit/
├── action.yml
├── scripts/
│   ├── compute-version.sh   # derive next semver from conventional commits + git tags
│   ├── changelog.sh         # generate changelog entries for this release
│   └── tag-release.sh       # create git tag + GitHub release
└── README.md
```

## action.yml sketch

```yaml
name: shipkit
description: Compute version, generate changelog, tag and release
inputs:
  token:
    required: true
    description: GitHub token with contents:write permission
outputs:
  version:
    description: The version that was computed (e.g. "1.2.3")
    value: ${{ steps.compute.outputs.version }}
  released:
    description: "true if a new release was created, false if no-bump"
    value: ${{ steps.compute.outputs.released }}
runs:
  using: composite
  steps:
    - name: Compute version
      id: compute
      shell: bash
      run: ${{ github.action_path }}/scripts/compute-version.sh
    - name: Generate changelog
      shell: bash
      run: ${{ github.action_path }}/scripts/changelog.sh
    - name: Tag and release
      shell: bash
      run: ${{ github.action_path }}/scripts/tag-release.sh
      env:
        GH_TOKEN: ${{ inputs.token }}
```

## Caller pattern

Callers use shipkit as one job, then gate downstream jobs (e.g. PyPI publish) on its `released` output:

```yaml
jobs:
  release:
    runs-on: ubuntu-latest
    outputs:
      version: ${{ steps.shipkit.outputs.version }}
      released: ${{ steps.shipkit.outputs.released }}
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0      # full history needed for version computation
      - uses: org/shipkit@v1
        id: shipkit
        with:
          token: ${{ secrets.GITHUB_TOKEN }}

  publish:
    needs: release
    if: needs.release.outputs.released == 'true'
    steps:
      - uses: actions/publish-to-pypi@v1   # or equivalent
        ...
```

The `if: released == 'true'` guard is important — it prevents publish from running on commits that produce no new version (e.g. `docs:` or `no-bump:` prefixed commits).

## Key decisions

- **Composite over JavaScript/Docker**: logic is already shell; no rewrite needed. Docker startup (~30s) ruled out for a frequently-run release step.
- **Outputs over tag-trigger**: exposing `released` as an output (rather than triggering downstream jobs off the pushed tag) keeps the full pipeline visible in one workflow file and avoids re-implementing the "did this cut a release?" guard downstream.
- **`fetch-depth: 0` in callers**: version computation reads git tags and commit history; callers must do a full checkout.
- **`${{ github.action_path }}`**: use this to reference scripts inside the action repo, so paths resolve correctly regardless of caller location.

## Versioning / publishing

Tag the shipkit repo with semver tags (`v1`, `v1.2.3`) so callers can pin to a major version (`org/shipkit@v1`) and get non-breaking updates automatically. Use shipkit to release itself.
