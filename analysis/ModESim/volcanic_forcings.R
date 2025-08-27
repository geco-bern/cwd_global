#read in libraries
library(ncdf4)
library(dplyr)

###########Volcanic Forcings:
# In both files, AOD should have three dimensions: time, latitude, and wavelength.
# You need to check some papers on eruption to see which wavelengths are relevant and typically used (550nm)
#lat lon and time info from netcdf files
##read in data

# Define the base path
base_path <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/ModESim_forcings/"
years <- 1850:2009  # Define range of years

# Initialize an empty list to store data
all_data <- list()

# Loop through each year and process the NetCDF file
for (year in years) {
  # Construct file path dynamically with the correct filename format
  file_path <- paste0(base_path, "1850_1/strat_aerosol_ir_T63_", year, ".nc")

  # Check if file exists before processing
  if (file.exists(file_path)) {
    nc_forcings <- nc_open(file_path)

    # Extract variables
    aod <- ncvar_get(nc_forcings, varid="aod")[6,,]  # Index 6 for 500nm
    lat <- ncvar_get(nc_forcings, varid="lat")
    time_raw <- ncvar_get(nc_forcings, varid="time")

    # Convert time variable
    reference_date <- as.Date("1850-01-01")  # Metadata reference date
    converted_dates <- reference_date + time_raw  # Convert time to actual dates

    # Close NetCDF file
    nc_close(nc_forcings)

    # Store extracted data in a data frame
    df <- data.frame(
      Year = year,
      Date = rep(converted_dates, times = length(lat)),  # Expand dates for latitudes
      Latitude = rep(lat, each = length(converted_dates)),  # Expand latitude
      AOD = as.vector(aod)  # Flatten AOD array
    )

    # Append to list
    all_data[[as.character(year)]] <- df
  } else {
    print(paste("File not found:", file_path))
  }
}

# Combine all years into one data frame
final_data <- bind_rows(all_data)

saveRDS(final_data, "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/ModESim_forcings/1850_1/final_aod_data.rds")


##############
# #Load again with
volc_1850 <- readRDS("/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/ModESim_forcings/1850_1/final_aod_data.rds")

# Summarize AOD by year
volc_summary <- volc_1850 %>%
  group_by(Year) %>%
  summarise(mean_AOD = mean(AOD, na.rm = TRUE))

#plot:
# Load necessary libraries
library(ggplot2)
library(dplyr)

# Create the ggplot line plot
ggplot(volc_summary, aes(x = Year, y = mean_AOD)) +
  geom_line(color = "blue") +  # Line plot with blue color
  #geom_point(color = "red", size = 1) +  # Optional: Add points for each year
  labs(title = "Mean Annual AOD (1420-1850)",
       x = "Year",
       y = "Annual Mean AOD") +
  theme_minimal()  # Use a clean minimal theme

## save annual mean AOD for 1850 period as:
saveRDS(volc_summary, "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/ModESim_forcings/1850_1/mean_aod.rds")
write.csv(volc_summary, "~/cwd_global/data/AOD_annual_mean_1850epoch.csv")
