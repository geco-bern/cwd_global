#' Calculates atmospheric pressure
#'
#' Calculates atmospheric pressure as a function of elevation, by default assuming
#' standard atmosphere (101325 Pa at sea level)
#'
#' @param elv Elevation above sea-level (m.a.s.l.)
#' @param patm0 (Optional) Atmospheric pressure at sea level (Pa), defaults to 101325 Pa.
#'
#' @details The elevation-dependence of atmospheric pressure is computed by
#' assuming a linear decrease in temperature with elevation and a mean
#' adiabatic lapse rate (Berberan-Santos et al., 1997):
#' \deqn{
#'    p(z) = p0 ( 1 - Lz / TK0) ^ ( g M / (RL) )
#' }
#' where \eqn{z} is the elevation above mean sea level (m, argument \code{elv}),
#' \eqn{g} is the gravity constant (9.80665 m s-2), \eqn{p0} is the atmospheric
#' pressure at 0 m a.s.l. (argument \code{patm0}, defaults to 101325 Pa),
#' \eqn{L} is the mean adiabatic lapse rate (0.0065 K m-2),
#' \eqn{M} is the molecular weight for dry air (0.028963 kg mol-1),
#' \eqn{R} is the universal gas constant (8.3145 J mol-1 K-1), and \eqn{TK0}
#' is the standard temperature (298.15 K, corresponds to 25 deg C).
#'
#' @return A numeric value for \eqn{p}
#'
#' @examples print("Standard atmospheric pressure, in Pa, corrected for 1000 m.a.s.l.:")
#' print(calc_patm(1000))
#'
#' @references  Allen, R. G., Pereira, L. S., Raes, D., Smith, M.:
#'              FAO Irrigation and Drainage Paper No. 56, Food and
#'              Agriculture Organization of the United Nations, 1998
#'
#' @export
#'

calc_patm <- function( elv, patm0){

  # Define constants:
  kTo <- 298.15    # base temperature, K (Prentice, unpublished)
  kL  <- 0.0065    # adiabiatic temperature lapse rate, K/m (Allen, 1973)
  kG  <- 9.80665   # gravitational acceleration, m/s^2 (Allen, 1973)
  kR  <- 8.3145    # universal gas constant, J/mol/K (Allen, 1973)
  kMa <- 0.028963  # molecular weight of dry air, kg/mol (Tsilingiris, 2008)

  # Convert elevation to pressure, Pa:
  out <- patm0*(1.0 - kL*elv/kTo)^(kG*kMa/(kR*kL))

  return(out)
}


##################Calculate patm from SLP and elevation from ModE-Sim
##set working directory

## Files located on ClimCal server
setwd("~/ModE-Sim/outdata/set_1850-1/abs/m016/by_year")

library(ncdf4) # package for netcdf manipulation
library(raster) # package for raster manipulation
library(rgdal) # package for geospatial analysis
library(ggplot2) # package for plotting
library(fs) # For directory operations (optional)


##read in data
# input_file_SLP <- "ModE-Sim_set_1850-1_m002_1850_mon.toasurf.nc"
# nc_SLP <- nc_open(input_file_SLP)
# slp = ncvar_get(nc_SLP, varid="slp")
# print(nc_SLP)
# nc_close(nc_SLP)

input_file_elv <- "~/ModE-Sim/inputfiles/T63GR15_jan_surf.nc"
nc_elv <- nc_open(input_file_elv)
elv = ncvar_get(nc_elv, varid="OROMEA")
print(nc_elv)
nc_close(nc_elv)

###call function to calculate patm:
#calc_patm(elv, slp[1:2])   #have to loop through slp? basically have 12 months

# Define the function to process the slp array with time dimension
determine_surface_pressure <- function(elv, slp) {
  # Check dimensions of elv and slp
  stopifnot(dim(elv)[1] == dim(slp)[1] && dim(elv)[2] == dim(slp)[2])

  # Initialize array to store the result
  num_times <- dim(slp)[3]
  pressure_results <- array(NA, dim = c(dim(elv)[1], dim(elv)[2], num_times))

  # Loop over the time dimension
  for (t in 1:num_times) {
    # Calculate surface pressure for each time slice
    pressure_results[,,t] <- calc_patm(elv, slp[,,t])
  }

  return(pressure_results)
}

# Example usage
# Assuming `elv` and `slp` are already loaded into your R environment
# elv <- matrix of dimensions [1:192, 1:96]
# slp <- array of dimensions [1:192, 1:96, 1:12]
#
# result <- determine_surface_pressure(elv, slp)
#

######################################################
#######Loop through all files and store like netrad on scratch2
# Define the directory path where the NetCDF files will be saved
dir_path <- "~/scratch2/surfaceP/m016_1850_1" # Replace with your desired directory path
# Check if the directory exists, and create it if it doesn't
if (!dir_exists(dir_path)) {
  dir_create(dir_path)
  cat("Directory created:", dir_path, "\n")
} else {
  cat("Directory already exists:", dir_path, "\n")
}

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
  slp <- ncvar_get(nc_data, varid="slp")
  nc_close(nc_data) # Close the input file after reading dimensions and data

  # Calculate surface pressure by using function determine_surface_pressure
  patm <- determine_surface_pressure(elv, slp)

  # Define missing value
  mv <- -9999

  # Define dimensions for the new NetCDF file
  lon1 <- ncdim_def(name = "longitude", units = "degrees_east", vals = lon)
  lat2 <- ncdim_def(name = "latitude", units = "degrees_north", vals = lat)
  time3 <- ncdim_def(name = "Time", units = "months", vals = 1:12, unlim = TRUE)

  # Define the variable for the new NetCDF file
  var_netrad <- ncvar_def(name = "patm", units = "Pa",
                          dim = list(lon1, lat2, time3),
                          longname = "surface pressure",
                          missval = mv)

  # Define the filename for the new NetCDF file for the current year
  filename <- sprintf("ModE-Sim_set_1850-1_m016_%d_mon.patm.nc", year)

  # Combine directory path and filename to get the full file path
  file_path <- file.path(dir_path, filename)

  # Create the new NetCDF file and add the variable
  tryCatch({
    ncnew <- nc_create(file_path, vars = var_netrad)
    print(paste("NetCDF file created successfully at:", file_path))

    # Write the data to the variable
    ncvar_put(ncnew, var_netrad, patm)

    # Close the NetCDF file
    nc_close(ncnew)
    print("Data written successfully!")
  }, error = function(e) {
    cat("Error in processing file", filename, ":", e$message, "\n")
  })
}

#continue but with rerun file for 1435
