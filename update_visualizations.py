import packaging.version
import pathlib

import click
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

dl_per_day_7d_avg_label = "Downloads per day (7-day average)"


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
    _render_latest_n(df, 3, vis_loc_latest_3)

    vis_name_latest = stats_loc.with_suffix(".latest.png").name
    vis_loc_latest = vis_dir / vis_name_latest
    _render_latest_n(df, 1, vis_loc_latest, use_weekday_labels=True, aspect=3)


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

    df.set_index(["date", "version", "os"], inplace=True)
    df["download_count_diff"] = df.groupby(["version", "os"]).download_count.diff()

    df[dl_per_day_7d_avg_label] = float("nan")
    for key, group in df.groupby(["version", "os"]):
        df.loc[
            group.index, dl_per_day_7d_avg_label
        ] = group.download_count_diff.rolling("7D", on=group.index.levels[0]).mean()

    return df


def _render_all(df, vis_loc, *, use_weekday_labels=False, **kwargs):
    col = "version"
    col_order = sorted(df.index.get_level_values(col).unique(), reverse=True)
    hue = "os"
    hue_order = sorted(df.index.get_level_values(hue).unique())

    fg = sns.relplot(
        kind="line",
        data=df,
        x="date",
        y=dl_per_day_7d_avg_label,
        hue=hue,
        hue_order=hue_order,
        col=col,
        col_order=col_order,
        **kwargs,
    )
    _set_date_formatter(fg, use_weekday_labels=use_weekday_labels)
    fg.savefig(vis_loc)


def _render_latest_n(df, n, *args, **kwargs):
    col = "version"
    col_order = sorted(df.index.get_level_values("version").unique(), reverse=True)
    latest_n = col_order[:n]

    df = df.loc[:, latest_n, :]

    _render_all(df, *args, **kwargs)


def _extract_major_minor_version(version):
    version = packaging.version.parse(version)
    version = packaging.version.parse(f"{version.major}.{version.minor}")
    return version


def _set_date_formatter(facet_grid, use_weekday_labels):
    for ax in facet_grid.axes.flat:
        if ax.get_xlabel():
            if use_weekday_labels:
                # set ticks every Monday
                monday_locator = mdates.WeekdayLocator(byweekday=(mdates.MO,))
                ax.xaxis.set_major_locator(monday_locator)
                ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(monday_locator))
            plt.setp(ax.get_xticklabels(), rotation=90)


if __name__ == "__main__":
    matplotlib.use("agg")
    sns.set_theme(style="whitegrid")
    update_vis()
