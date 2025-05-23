setwd("~")

library(ncdf4) # package for netcdf manipulation
library(raster) # package for raster manipulation
library(rgdal) # package for geospatial analysis
library(ggplot2) # package for plotting
library(fs) # For directory operations (optional)

##set working directory
setwd("~/ModE-Sim/outdata/set_1850-1/abs/m016/by_year")

# Define the directory path where the NetCDF file will be saved
dir_path <- "~/scratch2/netradiation/m016_1850_1" # Replace with your desired directory path

# Check if the directory exists, and create it if it doesn't
if (!dir_exists(dir_path)) {
  dir_create(dir_path)
  cat("Directory created:", dir_path, "\n")
} else {
  cat("Directory already exists:", dir_path, "\n")
}

##read in data
# input_file <- "ModE-Sim_set_1850-1_m001_1435_mon.toasurf.nc"
# nc_data <- nc_open(input_file)
#
#
# ####extract netcdf data:
# # Read dimensions from the existing NetCDF file
# lon <- ncvar_get(nc_data, varid="lon")
# lat <- ncvar_get(nc_data, varid="lat")
# time <- ncvar_get(nc_data, varid="time")
#
# solar_rad =ncvar_get(nc_data, varid="srads")
# thermal_rad =ncvar_get(nc_data, varid="trads")
# nc_close(nc_data) # Close the input file after reading dimensions
#
# ##calculate net radiation by addition of solar and thermal radiation
# net_rad = solar_rad + thermal_rad
#
# print(net_rad)

# #####################################################################
# #####################################################################
# # Define missing value
# mv <- -9999
#
# # Define dimensions for the new NetCDF file
# lon1 <- ncdim_def("longitude", "degrees_east", lon)
# lat2 <- ncdim_def("latitude", "degrees_north", lat)
# time3 <- ncdim_def("Time", "months", 1:12, unlim = TRUE)
#
# # Define the variable for the new NetCDF file
# var_netrad <- ncvar_def("netrad", "W m-2", list(lon1, lat2, time3),
#                         longname = "net surface radiation", missval = mv)
#
# # Create the new NetCDF file and add the variable
# filename <- "ModE-Sim_set_1850-1_m001_1435_mon.netrad.nc"
#
# # Combine directory path and filename to get the full file path
# file_path <- file.path(dir_path, filename)
#
# # Create the new NetCDF file and add the variable
# ncnew <- nc_create(file_path, vars = var_netrad)
#
# # Write the data to the variable
# ncvar_put(ncnew, var_netrad, net_rad)
#
# # Close the NetCDF file
# nc_close(ncnew)
#
# nc_test2 = get(ncnew, varid="netrad")

######try opening new netcdf file with netrad for 1850 m001:

# ##read in data
# ##set working directory
# setwd("~/scratch2/netradiation")
# input_file <- "ModE-Sim_set_1850-1_m001_1850_mon.netrad.nc"
# nc_test <- nc_open(input_file)
# rad_test =ncvar_get(nc_test, varid="netrad")
# rad_test
############################
#loop the whole process for all years
#https://stackoverflow.com/questions/43477779/fail-to-loop-in-creating-multiple-ncdf4-files-from-analysis

library(ncdf4)
library(fs) # For directory operations (optional)

# Define the directory path where the NetCDF files will be saved
dir_path <- "~/scratch2/netradiation/m016_1850_1" # Replace with your desired directory path

# Define the range of years (or time periods) for your input files
years <- 1850:2009

# Loop through each year
for (year in years) {
  # Define the input file path for the current year
  input_file <- sprintf("ModE-Sim_set_1850-1_m016_%d_mon.toasurf.nc", year)

  # Open the existing NetCDF file to read dimensions and data
  nc_data <- nc_open(input_file)

  # Read dimensions from the existing NetCDF file
  lon <- ncvar_get(nc_data, varid="lon")
  lat <- ncvar_get(nc_data, varid="lat")
  time <- ncvar_get(nc_data, varid="time")

  # Extract the solar and thermal radiation data
  solar_rad <- ncvar_get(nc_data, varid="srads")
  thermal_rad <- ncvar_get(nc_data, varid="trads")
  nc_close(nc_data) # Close the input file after reading dimensions and data

  # Calculate net radiation by addition of solar and thermal radiation
  net_rad <- solar_rad + thermal_rad

  # Define missing value
  mv <- -9999

  # Define dimensions for the new NetCDF file
  lon1 <- ncdim_def(name = "longitude", units = "degrees_east", vals = lon)
  lat2 <- ncdim_def(name = "latitude", units = "degrees_north", vals = lat)
  time3 <- ncdim_def(name = "Time", units = "months", vals = 1:12, unlim = TRUE)

  # Define the variable for the new NetCDF file
  var_netrad <- ncvar_def(name = "netrad", units = "W m-2",
                          dim = list(lon1, lat2, time3),
                          longname = "net surface radiation",
                          missval = mv)

  # Define the filename for the new NetCDF file for the current year
  filename <- sprintf("ModE-Sim_set_1850-1_m016_%d_mon.netrad.nc", year)

  # Combine directory path and filename to get the full file path
  file_path <- file.path(dir_path, filename)

  # Create the new NetCDF file and add the variable
  tryCatch({
    ncnew <- nc_create(file_path, vars = var_netrad)
    print(paste("NetCDF file created successfully at:", file_path))

    # Write the data to the variable
    ncvar_put(ncnew, var_netrad, net_rad)

    # Close the NetCDF file
    nc_close(ncnew)
    print("Data written successfully!")
  }, error = function(e) {
    cat("Error in processing file", filename, ":", e$message, "\n")
  })
}
