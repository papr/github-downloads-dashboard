import pathlib

import click
import pandas as pd
from gdc.gdc import Github


@click.command()
@click.argument("github_account", nargs=1)
@click.option(
    "-o",
    "--out",
    "output_dir",
    type=click.Path(),
    default="statistics",
    show_default=True,
)
def update_statistics(github_account, output_dir):
    stats_by_repo = _get_statistics(github_account)
    _save_stats_to_parquet(stats_by_repo, output_dir)


def _get_statistics(github_account):
    github = Github()
    stats = github.get_releases_by_user(github_account)

    stats_by_repo = {
        name: pd.DataFrame(dc, columns=["asset", "download_count"])
        for name, dc in stats
    }

    for df in stats_by_repo.values():
        df["asset"] = df["asset"].astype(pd.StringDtype())

    return stats_by_repo


def _save_stats_to_parquet(stats_by_repo, output_dir):
    stats_dir = pathlib.Path(output_dir)
    stats_dir.mkdir(exist_ok=True)

    now = pd.to_datetime("now")

    for repo, df in stats_by_repo.items():
        df["date"] = now

        stats_loc = stats_dir / f"{repo}.parquet"
        if stats_loc.exists():
            prev_data = pd.read_parquet(stats_loc)
            df = pd.concat([prev_data, df])
        df.to_parquet(stats_loc)


if __name__ == "__main__":
    update_statistics()
