#read in libraries
library(ncdf4)

###########Volcanic Forcings:
# In both files, AOD should have three dimensions: time, latitude, and wavelength.
# You need to check some papers on eruption to see which wavelengths are relevant and typically used (550nm)
#lat lon and time info from netcdf files
##read in data

#AOD years 1000 C.E. to 1900 C.E.:
input_file <- "/mnt/climstor/ERC_PALAEO/ModE-Sim/inputfiles/PALAEO-RA/forcings/PMIP4_volcano/eva_holo2.2_forcing_echam_T63_ir_1000-1900.nc"

nc_forcings <- nc_open(input_file)
# print(nc_forcings)


# #AOD years 1850 C.E. to 2024 C.E.:
# input_file <-  "/mnt/climstor/exports/ERC_PALAEO/ModE-Sim/inputfiles/ECHAM6_pool/input/r0008/T63/volcano_aerosols/strat_aerosol_ir_T63_1850-2024.nc"



aod = ncvar_get(nc_forcings, varid="aod")[6,,] # index 6 for 500nm wavelength
lat = ncvar_get(nc_forcings, varid="lat")
time_1420 = ncvar_get(nc_forcings, varid="time")
# wl = ncvar_get(nc_forcings, varid="wl") #units of mu m
# wl_lo = ncvar_get(nc_forcings, varid="wl_lo") #units of nm
# wl_up = ncvar_get(nc_forcings, varid="wl_up") #units of nm
#

# # Convert to actual dates (days since 2001-01-01)
# reference_date <- as.Date("1400-1-1 00:00:00")
# time_dates_1420 <- reference_date + time_1850

# # Print the resulting dates
# print(time_dates)
nc_close(nc_forcings)

########### Calculate annual mean values for first epoch years:
# Define the years of interest
years_of_interest <- 1420:1850

# Find indices of columns corresponding to the desired years
selected_indices <- which(time_1420 %in% years_of_interest)

# Subset the matrix to include only those years
aod_selected <- aod[, selected_indices]
time_selected <- time_1420[selected_indices]  # Corresponding time values

# Compute the mean over latitudes (first dimension)
mean_latitude <- apply(aod_selected, 2, mean, na.rm = TRUE)

# Convert to a data frame for easier manipulation
df <- data.frame(year = time_selected, mean_value = mean_latitude)

# Compute the annual mean (since each year has 12 monthly values)
library(dplyr)
annual_mean <- df %>%
  group_by(year) %>%
  summarise(annual_mean_value = mean(mean_value, na.rm = TRUE))

# View result
print(annual_mean)


#plot:
# Load necessary libraries
library(ggplot2)
library(dplyr)

# Create the ggplot line plot
ggplot(annual_mean, aes(x = year, y = annual_mean_value)) +
  geom_line(color = "blue") +  # Line plot with blue color
  #geom_point(color = "red", size = 1) +  # Optional: Add points for each year
  labs(title = "Mean Annual AOD (1420-1850)",
       x = "Year",
       y = "Annual Mean AOD") +
  theme_minimal()  # Use a clean minimal theme
