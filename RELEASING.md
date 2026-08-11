# Releasing

Every release uses `./scripts/release.sh`. Do not bump versions, tag, or create GitHub Releases manually.

## One-time setup

- Install [GitHub CLI](https://cli.github.com/) (`gh`) and authenticate.
- Ensure PyPI [trusted publishing](https://docs.pypi.org/trusted-publishers/) is configured for this repo (`publish.yml` uses the `pypi` GitHub environment).

## Release steps

1. Add user-facing notes under `## [Unreleased]` in `CHANGELOG.md`.
2. Prepare the release PR:

   ```bash
   ./scripts/release.sh prepare patch   # or minor | major | 1.2.3
   ```

3. Merge the PR after CI passes (including the changelog check).
4. Publish from a clean default branch checkout:

   ```bash
   git checkout main   # or master for hotdata-marimo
   git pull
   ./scripts/release.sh publish
   ```

## What happens automatically

Pushing a `vX.Y.Z` tag triggers two workflows:

| Workflow | Purpose |
|----------|---------|
| `publish.yml` | Build wheel/sdist and publish to PyPI |
| `release.yml` | Create the GitHub Release with notes from `CHANGELOG.md` |

## If a release workflow fails

Both workflows also accept a manual re-run against an existing tag, so a failure
unrelated to the code — a stale action pin, a PyPI outage — does not require
deleting the tag or burning a version number:

```bash
gh workflow run "Publish to PyPI" --ref main -f tag=vX.Y.Z
gh workflow run "GitHub Release"  --ref main -f tag=vX.Y.Z
```

`--ref main` selects the workflow *definition* (so it picks up any fix landed
since the tag); the `tag` input selects what gets built and released. The two
refs serve different purposes, which is why they can differ.

A version is only spent once PyPI has accepted an upload. If the publish failed
before that, the same version can still be published — check with
`curl -s -o /dev/null -w '%{http_code}' https://pypi.org/pypi/<pkg>/<version>/json`
returning 404.

## Enforcement

- **PR check** (`check-release.yml`): if `pyproject.toml` version changes, `CHANGELOG.md` must contain a matching `## [X.Y.Z]` section.
- **Tag check** (`publish.yml`): the tag (without `v`) must match `[project].version` in `pyproject.toml`.
- **Publish guard** (`release.sh publish`): refuses to tag if the changelog section is missing.

Together, these make it hard to ship a version without changelog notes or a GitHub Release.
