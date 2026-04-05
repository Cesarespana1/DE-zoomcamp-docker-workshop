#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}

@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL username')
@click.option('--pg_pass', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--pg_port', default=5432, type=int, help='PostgreSQL port')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
@click.option('--target_table', default='zone_lookup', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_db, pg_port, chunksize, target_table):

    # Parameters are now passed via command line options

    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        iterator=True,
        chunksize= chunksize
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace')
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine, 
            if_exists='append')
        
if __name__ == "__main__":
    run()
