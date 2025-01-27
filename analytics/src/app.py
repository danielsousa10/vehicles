import sys
import logging
import click

from runner.analytics_runner import Runner

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [ANALYTICS - %(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/analytics/log/analytics.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

@click.group()
def main() -> None:
    """Utility tool to run analytic queries against Vehicles database"""
    pass

@click.command()
@click.option("-n",
                help="Number of rows to select",
                default=20, 
                show_default=True,
                type=int
            )
def select(n: int) -> None:
    """Select N rows from Trips table"""
    print(Runner().run_select(n))

@click.command()
@click.option("--name",
                help="Region name",
                default='',
                type=str
            )
def select_by_region(name: str) -> None:
    """Select weekly average number of trips by region"""
    print(Runner().run_select_by_region(name))

@click.command()
@click.option("--box",
                help="Bounding box. Format: (a,b),(c,d)",
                default='',
                type=str
            )
def select_by_box(box: str) -> None:
    """Select weekly average number of trips by bounding box"""
    print(Runner().run_select_by_box(box))

@click.command()
@click.option("-n",
                help="Top N most commonly regions",
                default='',
                type=int
            )
def select_latest_datasource(n: int) -> None:
    """From the top N most commonly regions, select the latest datasource"""
    print(Runner().run_select_top_region_latest_datasource(n))

@click.command()
def select_regions_cheap_mobile() -> None:
    """Select regions that include the 'cheap_mobile' datasource"""
    print(Runner().select_regions_cheap_mobile())


main.add_command(select)
main.add_command(select_by_region)
main.add_command(select_by_box)
main.add_command(select_latest_datasource)
main.add_command(select_regions_cheap_mobile)

if __name__ == "__main__":
    logging.info("Initializing application...")
    main()