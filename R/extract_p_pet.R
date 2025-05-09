library(dplyr)
library(purrr)
library(readr)
library(lubridate)

#in_fname <- in_fname[1]
######################### select deficit, p and precip for those selected years

# # Function to extract required columns and filter selected years
# extract_selected_data <- function(df, selected_years) {
#   df |>
#     mutate(year = lubridate::year(date)) |>  # Extract year from date
#     filter(year %in% selected_years) |>  # Filter for specific years
#     select(date, precip, pet, deficit)  # Keep only relevant columns
# }


extract_selected_data <- function(df, selected_years) {
  df |>
    mutate(
      year = lubridate::year(date),   # Extract year
      month = lubridate::month(date)  # Extract month
    ) |>
    filter(year %in% selected_years) |>  # Keep only selected years
    group_by(year, month) |>  # Group by year and month
    summarise(
      date = min(date),   # Use the first date of the month as a reference
      precip = mean(precip, na.rm = TRUE),
      pet = mean(pet, na.rm = TRUE),
      deficit = mean(deficit, na.rm = TRUE),
      .groups = "drop"  # Prevents grouping issues
    ) |>
    select(date, precip, pet, deficit)  # Keep only relevant columns
}

# Main function to process input file
process_cwd_extract_data <- function(
    in_fname,
    outdir,
    selected_years
){
  # Read RDS file
  df <- readr::read_rds(in_fname)

  # Apply extraction function to each element in the 'data' list-column
  out <- df |>
    mutate(data = purrr::map(
      data,
      ~extract_selected_data(.x$df, selected_years)
    ))

  # Define output file name
  path <- file.path(outdir, gsub('.rds', '_EXTRACTED.rds', basename(in_fname)))
  message(paste0("Writing file ", path, " ..."))

  # Write extracted data to file
  readr::write_rds(out, path)

  # No return to save memory
  return(NULL)
}

