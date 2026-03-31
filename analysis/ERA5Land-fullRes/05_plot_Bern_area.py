# In RStudio: reticulate::use_condaenv("cwd_era5land") # maybe Sys.unsetenv("RETICULATE_PYTHON") is also needed.
import numpy as np
import matplotlib.pyplot as plt
import xarray as xr
import pandas as pd
import time
import pathlib
import os, fnmatch
#from dask.distributed import Client, LocalCluster

#import dask.dataframe as dd
#import dask.array as da
#import netCDF4
#from datetime import datetime as dtdt

# cluster = LocalCluster(
#     #n_workers=10, # not specifying uses all available in SLURM
#     threads_per_worker=1,
#     memory_limit = 1.0, # If a float, that fraction of the system memory is used per worker,
#                         # meaning with 1.0 we allow one worker to use all memory.
#                         # see: https://docs.dask.org/en/latest/deploying-python.html#distributed.deploy.local.LocalCluster
#     dashboard_address = f":{curr_year}",
#     processes=True
# )
# print(client.dashboard_link, flush=True) # use this dashboard to follow progress

def list_netcdf_files(root_dir, pattern):
    netcdf_files = []
    for root, dirs, files in os.walk(root_dir):
        for filename in fnmatch.filter(files, pattern):
            if filename.endswith('.nc'):
                netcdf_files.append(os.path.join(root, filename))
    return netcdf_files

netcdf_files = list_netcdf_files(
    "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd_v2-doy-reset_netcdf/",
    '*.nc')
netcdf_files.sort()
[print(file) for file in netcdf_files];

# e.g. "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd_v2-doy-reset_netcdf/data_derived_03_daily_pcwd_v2-doy_2019_r-generated.nc"


#### open files
ds = xr.open_mfdataset(
    netcdf_files,
    combine='by_coords',
    # engine="h5netcdf",
    engine='netcdf4',
    parallel =True,
    chunks="auto")


# Check output visually:
# plt.switch_backend('WebAgg')
# plt.switch_backend('QtAgg')
# plt.switch_backend('TKAgg')
# plt.switch_backend('GTKAgg')
# plt.switch_backend('Qt4Agg')
# plt.switch_backend('WXAgg')
# plt.switch_backend('cairo')

plt.figure()
ds['pcwd_mm'].isel(lon = 76, lat = 700+467).plot()
# plt.show()
plt.savefig('testing1.png')

plt.figure()
ds['pcwd_mm'].isel(lon = slice(76,78), lat = 700+467).plot.line(x='time')
plt.savefig('testing1.png')

plt.figure()
ds['pcwd_mm'].isel(
    lon = slice(76,78), lat = slice(700+467, 700+469)
    ).stack(xy=('lat', 'lon')
    ).plot.line(x='time')
plt.savefig('testing2.png')



# # SHOWCASE HOW TO INTERACT AND PLOT: ###########################################
#     # subsetting Datasets with variable => DataArray with sel() or []
#     ds_hou['t2m']
#     ds_hou.t2m

#     # By numeric value (sel()):
#     ds_hou['t2m'].sel(longitude = 0.0)
#     ds_hou.t2m.sel(longitude = 0.0)
#     ds_lon_hou = ds_hou.sel(longitude = 0.0) # TODO: this will need to loop over all lon-values

#     # By position (isel() or [])
#     ds_hou.t2m[0,0,0] # following order of *-coordinates: ds_hou.t2m.coords (valid_time, latitude, longitude)
#     ds_hou.t2m.isel(valid_time=0, latitude=0, longitude=0)
#     #   ds_hou[0,0,0] # NOTE that doing this on a Dataset errors. But with isel works also for Datasets (containing multiple variables):
#     ds_hou.isel(valid_time=0, latitude=0, longitude=0)

#     # exmaple subset a single longitude slice for map2tidy
#     ds_hou.t2m[:,0,0] # following order of *-coordinates: ds_hou.t2m.coords (valid_time, latitude, longitude)
#     ds_hou.t2m[:,:,0] # lon = 0.0, following order of *-coordinates: ds_hou.t2m.coords (valid_time, latitude, longitude)
#     ds_hou.t2m[:,:,1] # lon = 0.1, following order of *-coordinates: ds_hou.t2m.coords (valid_time, latitude, longitude)
#     ds_hou.t2m[:,:,2] # lon = 0.2, following order of *-coordinates: ds_hou.t2m.coords (valid_time, latitude, longitude)

#     ds_lon_hou = ds_hou.isel(longitude=0) # do this in a loop over all longitude indices

#     # Plotting:
#     # t2m profile along latitude (-90 to +90)
#     plt.figure()
#     ds_hou.t2m[0,:,2].plot() # plot t2m vs latitude
#     plt.show()

#     plt.figure()
#     ds_hou.t2m[0,:,[2,12,102]].plot.line(x='latitude') # plot t2m vs latitude
#     # https://docs.xarray.dev/en/latest/user-guide/plotting.html#multiple-lines-showing-variation-along-a-dimension
#     plt.show()


#     # t2m profile along time (hourly)
#     plt.figure()
#     # find out which latitude is 200: ds_hou.t2m.latitude[700]
#     ds_hou.t2m[0:96,700,[12,102]].plot.line(x='valid_time') # plot t2m vs latitude
#     # ds_hou.t2m.sel(latitude = 20.0, method='nearest')[0:96, [2,12,102]].plot.line(x='valid_time') # plot t2m vs latitude
#     plt.show()

#     # 2D t2m maps of a time instant:
#     plt.figure()
#     ds_hou.t2m[0,:,0:500].plot()
#     plt.show()
#     # TODO: make title mutli-line
#     ############################################################################




# # ds_lon_hou = ds_hou.isel(longitude=0) # do this in a loop over all longitude indices

# ds_lon007_hou = ds_hou.sel(longitude=7.4, method = 'nearest') # TODO: uncomment, just for development
# ds_lon090_hou = ds_hou.sel(longitude=90, method = 'nearest') # TODO: uncomment, just for development
# ds_lon240_hou = ds_hou.sel(longitude=240, method = 'nearest') # TODO: uncomment, just for development
# ds_hou.chunks
# ds_lon_hou.chunks

