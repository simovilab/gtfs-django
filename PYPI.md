# Building and publishing to PyPI

Run these steps from the repository root with [uv installed](https://docs.astral.sh/uv/getting-started/installation/). You need a PyPI account with permission to publish `gtfs-django` and a [PyPI API token](https://pypi.org/manage/account/token/), preferably scoped to this project.

1. Choose a new, unpublished version. For example, increment the patch version in `pyproject.toml` (use `--bump minor` or `--bump major` when appropriate):

   ```bash
   uv version --bump patch
   ```

   PyPI does not allow replacing previously uploaded distribution files, even after deleting a release.

2. Install development dependencies and run the tests:

   ```bash
   uv sync
   uv run pytest
   ```

3. Build the source distribution and wheel using the configured Hatchling backend. Use a version-specific output directory to avoid uploading older releases accidentally:

   ```bash
   VERSION=$(uv version --short)
   uv build --no-sources --out-dir "dist/$VERSION"
   ```

   Review the generated `.tar.gz` and `.whl` files in `dist/<version>/` before publishing.

4. Store your PyPI API token in `.env` at the repository root:

   ```dotenv
   UV_PUBLISH_TOKEN=pypi-REPLACE_WITH_YOUR_COMPLETE_TOKEN
   ```

   Replace the placeholder with the complete token, including its `pypi-` prefix. `.env` is already excluded by `.gitignore`; never commit it or put the token in shell commands or `pyproject.toml`.

5. Load `.env`, export its variables, and upload only this release's artifacts. In zsh (the shell used for the successful 0.3.2 publication), run:

   ```zsh
   (
     unsetopt XTRACE VERBOSE
     set -a
     source .env && {
       set +a
       uv publish "dist/$VERSION/"*
     }
   )
   ```

   Run this in the same shell where `VERSION` was set in step 3. To publish an already-built release instead, first set `VERSION` to its directory name, for example `VERSION=0.3.2`. Both the wheel and source archive are uploaded.

   The subshell keeps the exported token out of the parent shell's environment, and tracing is disabled before loading secrets. Only source your own trusted `.env` file: `source` executes shell syntax.

   `uv publish` in the version used here does not support `--env-file`, so load the file through the shell as shown above. Do not pass `--token pypi-`: this overrides `UV_PUBLISH_TOKEN` with an incomplete token and causes a 403 authentication error. For CI, supply `UV_PUBLISH_TOKEN` through your CI secret store instead of a file.

6. Verify the release at [pypi.org/project/gtfs-django/](https://pypi.org/project/gtfs-django/) and check that the published version can be installed and imported without using the local checkout:

   ```bash
   uv run --no-project --with "gtfs-django==$VERSION" -- python -c "import gtfs"
   ```
