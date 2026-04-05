#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL username')
@click.option('--pg_pass', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--pg_port', default=5432, type=int, help='PostgreSQL port')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
@click.option('--target_table', default='yellow_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_db, pg_port, year, month, chunksize, target_table):

    # Parameters are now passed via command line options

    prefix = "https://d37ci6vzurychx.cloudfront.net"
    url = f"{prefix}/trip-data/green_tripdata_{year}-{month:02d}.parquet"
    engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    df = pd.read_parquet(url)

    df.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists='replace')

    df.to_sql(
        name=target_table,
        con=engine, 
        if_exists='append',
        chunksize= chunksize)
        
if __name__ == "__main__":
    run()





