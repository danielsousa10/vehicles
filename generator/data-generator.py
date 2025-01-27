import random
import os
import sys
import uuid

from collections import defaultdict
from typing import Tuple, Dict, List
from datetime import timedelta, datetime
from pathlib import Path

DATA_PATH = os.path.join(Path(__file__).parents[1], 'data')
SAMPLE_FILE = "trips.csv"
HEADER = "region,origin_coord,destination_coord,datetime,datasource\n"

def read_sample() -> str:
    return Path(os.path.join(DATA_PATH, SAMPLE_FILE)).read_text()

def get_mappings() -> Tuple:
    transform = lambda x: {k: list(v) for k, v in x.items()}

    rows = read_sample().split("\n")

    region_origin = defaultdict(set)
    region_dest = defaultdict(set)
    datasources = set()

    for r in rows[1:]:
        cols = r.split(",")
        if len(cols) < 5:
            continue

        region, origin, dest, _, source = cols
        region_origin[region].add(origin)
        region_dest[region].add(dest)
        datasources.add(source)

    region_origin = transform(region_origin)
    region_dest = transform(region_dest)
    return region_origin, region_dest, list(datasources)

def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)

    return start + timedelta(seconds=random_second)

def generate_row(region_origin: Dict, region_dest: Dict, datasources: List) -> str:
    region = random.choice(list(region_origin.keys()))
    origin = random.choice(region_origin[region])
    datasource = random.choice(datasources)

    d1 = datetime.strptime('1/1/2020 1:01 AM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('1/1/2022 1:01 AM', '%m/%d/%Y %I:%M %p')
    dt = random_date(d1, d2)

    dest = random.choice(region_dest[region])
    while origin == dest:
        dest = random.choice(region_dest[region])

    return f"{region},{origin},{dest},{dt},{datasource}\n"

def generate(rows: int) -> None:
    print(f"generating file with {rows} trips...")
    region_origin, region_dest, datasources = get_mappings()
    
    file_name = uuid.uuid4().hex + "_.csv"
    file_full_name = os.path.join(DATA_PATH, file_name)

    file = open(file_full_name, "a")
    file.write(HEADER)

    for n in range(rows):
        if not n % 10000:
            print(f"generated {n} rows...")
        file.write(generate_row(region_origin, region_dest, datasources))

    file.close()
    print(f"Generated {rows} at file {file_name}!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError("Missing number of rows.")

    num_rows = sys.argv[1]
    generate(int(num_rows))