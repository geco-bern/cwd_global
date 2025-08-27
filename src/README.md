This folder contains .sh files 
- `ERA5Land/` and `ModESim/` contain scripts to call (parallized) R-scripts from remote computing nodes on Ubelix.
- `CDO/` contains scripts to call CDO to manipulate climate data.
    - `CDO/ERA5-Land` contains scripts that regrid high resolution data to lower resolution in order to match ModE-Sim. Scripts to calculate net radiation from thermal and solar radiation are also included. 
    - `CDO/ModE-Sim` contains a script to extract precipitation (precip) and surface temperature (tsurf) from the raw GRIB files. Data is stored on ClimCal.
