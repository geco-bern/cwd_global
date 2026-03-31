import pandas as pd
import xarray as xr
import dask.dataframe as dd
import os
import numpy as np
import netCDF4
from datetime import datetime as dtdt

import dask.array as da
from dask.distributed import Client, LocalCluster

import argparse

def main():
    parser = argparse.ArgumentParser(description="Convert Parquet to NetCDF for a given year.")
    parser.add_argument("--year", type=int, required=True, help="Year to process (e.g., 2022)")
    parser.add_argument("--parquet_path", type=str, default=None, help="Path to input Parquet directory")
    parser.add_argument("--out_nc", type=str, default=None, help="Output NetCDF file path")

    args = parser.parse_args()

    curr_year = args.year
    parquet_path = args.parquet_path
    out_nc = args.out_nc
    print(out_nc)

    cluster = LocalCluster(
        #n_workers=10, # not specifying uses all available in SLURM
        threads_per_worker=1,
        memory_limit = 1.0, # If a float, that fraction of the system memory is used per worker,
                            # meaning with 1.0 we allow one worker to use all memory.
                            # see: https://docs.dask.org/en/latest/deploying-python.html#distributed.deploy.local.LocalCluster
        dashboard_address = f":{curr_year}",
        processes=True
    )
    print(cluster, flush=True)
    client = Client(cluster)
    print(client, flush=True)
    print(client.dashboard_link, flush=True) # use this dashboard to follow progress

    # Different ways how to open parquet files
    # ddf = pd.read_parquet(parquet_path) (using pandas: non-lazy)
    # ddf = dd.read_parquet(parquet_path) (using dask: lazy)
    ddf = dd.read_parquet(parquet_path, engine='pyarrow', partitioning="hive") # (using dask: lazy and spezifying engine)
    # ddf = dd.read_parquet(
    #     parquet_path, 
    #     [('year', '==', '2024')], 
    #     engine='pyarrow', 
    #     partitioning="hive") # see https://github.com/dask/dask/issues/8650 # (direct filtering upon opening, errors in our case??)

    # Filter the relevant year 
    # (Cleanest would be the filter argument in read_parquet() but not working.
    # Therefore, this should achieve the same thing, but slower than the filter argument.)
    print(f"{dtdt.now().strftime('%Y-%m-%d %H:%M:%S')}: Attempting to load subset into memory", flush=True)    
    ddf2 = ddf[ddf['year'] == curr_year].persist()  # Lazy, efficient row-group filter

    # Transform to xarray data set `ds`
    # NOTE: did not find a way how to go lazily from Dask DataFrame to xarray. 
    #       There exists an open PR since 2021.
    #       Therefore we just do it in-memory (non-lazy) with `ddf2`, i.e. after subsetting by year:
    df_year = ddf2.compute() # compute loads it into memory
    print(f"{dtdt.now().strftime('%Y-%m-%d %H:%M:%S')}: Finished loading subset into memory", flush=True)

    print(f"{dtdt.now().strftime('%Y-%m-%d %H:%M:%S')}: Start preparing for output: set_index", flush=True)
    ds = df_year.set_index(['lat', 'lon','date'])
    print(f"{dtdt.now().strftime('%Y-%m-%d %H:%M:%S')}: Start preparing for output: to_xarray", flush=True)
    ds = ds.to_xarray()
    print(f"{dtdt.now().strftime('%Y-%m-%d %H:%M:%S')}: Start preparing for output: drop_vars", flush=True)
    ds = ds.drop_vars(['LON_str','year'])
    print(f"{dtdt.now().strftime('%Y-%m-%d %H:%M:%S')}: Start preparing for output: chunk", flush=True)
    ds = ds.chunk({"lat": 18, "lon": 36, "date": 30})

    # fix date type for netcdf
    print(f"{dtdt.now().strftime('%Y-%m-%d %H:%M:%S')}: Start preparing for output: assign_coords", flush=True)
    ds = ds.assign_coords(date=("date", pd.to_datetime(ds.date.values)))

    # Output xarray as NetCDF (or zarr, or ...)
    # define chunking and compression:
    encoding = {'pcwd_mm': {
        'shuffle': True,
        'complevel': 3,
        'zlib': True,
        'chunksizes': (18, min(len(ds.lon),36), 30)
    }}

    print(f"{dtdt.now().strftime('%Y-%m-%d %H:%M:%S')}: Start writing output: to_netcdf", flush=True)
    ds.to_netcdf(
        out_nc,
        encoding=encoding,
        engine="netcdf4"
    )
    print(f"{dtdt.now().strftime('%Y-%m-%d %H:%M:%S')}: Finished writing output: to_netcdf", flush=True)

if __name__ == "__main__":
    main()