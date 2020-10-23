import packaging.version
import pathlib

import click
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
    show_default=True
)
def update_vis(statistics, output_dir):
    statistics_dir = pathlib.Path(statistics)
    for stats_loc in statistics_dir.glob("pupil.parquet"):
        _render_vis(stats_loc, output_dir)

def _render_vis(stats_loc, output_dir):
    df = pd.read_parquet(stats_loc)

    # preprocess assets
    df = df.loc[df["asset"].str.startswith("pupil")]
    split = df["asset"].str.split("_")
    version_os = split.apply(lambda parts: pd.Series(parts[1:3], index=["version", "os"]))
    df = pd.concat([df, version_os], axis="columns")
    # version
    df["version"] = split.apply(lambda parts: _extract_major_minor_version(parts[1].split("-")[0]))
    # operating system
    df["os"] = split.apply(lambda parts: parts[2])
    df = df.loc[df.os.isin(["macos", "linux", "windows"])]

    col = "version"
    col_order = sorted(df[col].unique(), reverse=True)

    sns.set_context("talk")
    fg = sns.relplot(
        kind="line",
        data=df,
        x="date",
        y="download_count",
        hue="os",
        col=col,
        col_order=col_order,
        col_wrap=5,
    )
    
    vis_dir = pathlib.Path(output_dir)
    vis_dir.mkdir(exist_ok=True)
    vis_loc = vis_dir / stats_loc.with_suffix(".png").name
    fg.savefig(vis_loc)

def _extract_major_minor_version(version):
    version = packaging.version.parse(version)
    version = packaging.version.parse(f"{version.major}.{version.minor}")
    return version

    
if __name__ == '__main__':
    update_vis()