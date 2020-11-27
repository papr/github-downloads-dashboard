import packaging.version
import pathlib

import click
import matplotlib.dates as mdates
import pandas as pd
import seaborn as sns

sns.set_theme(style="whitegrid")


@click.command()
@click.argument("statistics", type=click.Path(exists=True))
@click.option(
    "-o",
    "--out",
    "output_dir",
    type=click.Path(),
    default="visualizations",
    show_default=True,
)
def update_vis(statistics, output_dir):
    statistics_dir = pathlib.Path(statistics)
    for stats_loc in statistics_dir.glob("pupil.parquet"):
        _load_and_render(stats_loc, output_dir)


def _load_and_render(stats_loc, output_dir):
    df = _load(stats_loc)

    sns.set_context("talk")

    vis_dir = pathlib.Path(output_dir)
    vis_dir.mkdir(exist_ok=True)

    vis_name_all = stats_loc.with_suffix(".all.png").name
    vis_loc_all = vis_dir / vis_name_all
    _render_all(df, vis_loc_all, col_wrap=5)

    vis_name_latest_3 = stats_loc.with_suffix(".latest-3.png").name
    vis_loc_latest_3 = vis_dir / vis_name_latest_3
    _render_latest_3(df, vis_loc_latest_3)


def _load(stats_loc):
    df = pd.read_parquet(stats_loc)

    # preprocess assets
    df = df.loc[df["asset"].str.startswith("pupil")]
    split = df["asset"].str.split("_")
    version_os = split.apply(
        lambda parts: pd.Series(parts[1:3], index=["version", "os"])
    )
    df = pd.concat([df, version_os], axis="columns")
    # version
    df["version"] = split.apply(
        lambda parts: _extract_major_minor_version(parts[1].split("-")[0])
    )
    # operating system
    df["os"] = split.apply(lambda parts: parts[2])
    df = df.loc[df.os.isin(["macos", "linux", "windows"])]
    return df


def _render_all(df, vis_loc, col_wrap=None):
    col = "version"
    col_order = sorted(df[col].unique(), reverse=True)

    fg = sns.relplot(
        kind="line",
        data=df,
        x="date",
        y="download_count",
        hue="os",
        col=col,
        col_order=col_order,
        col_wrap=col_wrap,
    )
    _set_date_formatter(fg)
    fg.savefig(vis_loc)


def _render_latest_3(df, vis_loc):
    col = "version"
    col_order = sorted(df[col].unique(), reverse=True)
    latest_3 = col_order[:3]

    df = df.loc[df.version.isin(latest_3)]

    _render_all(df, vis_loc)


def _extract_major_minor_version(version):
    version = packaging.version.parse(version)
    version = packaging.version.parse(f"{version.major}.{version.minor}")
    return version


def _set_date_formatter(facet_grid):
    for ax in facet_grid.axes.flat:
        if ax.get_xlabel():
            # set ticks every week
            monday_locator = mdates.WeekdayLocator(byweekday=(mdates.MO,))
            ax.xaxis.set_major_locator(monday_locator)
            # set major ticks format
            ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(monday_locator))


if __name__ == "__main__":
    update_vis()
