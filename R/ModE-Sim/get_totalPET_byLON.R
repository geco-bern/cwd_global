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


extract_totalPET <- function(df) {
  df |>
    mutate(year = lubridate::year(date),   # Extract year
    ) |>
    group_by(year) |>  # Group by year
    summarise(
      tot_pet = sum(pet, na.rm = TRUE),
      .groups = "drop"  # Prevents grouping issues
    )
}

# Main function to process input file
get_totPET_byLON <- function(
    in_fname,
    outdir
){
  # Read RDS file
  df <- readr::read_rds(in_fname)

  # Apply extraction function to each element in the 'data' list-column
  out <- df |>
    mutate(data = purrr::map(
      data,
      ~extract_totalPET(.x$df)
    ))

  # Define output file name
  path <- file.path(outdir, gsub('.rds', '_tot_pet.rds', basename(in_fname)))
  message(paste0("Writing file ", path, " ..."))

  # Write extracted data to file
  readr::write_rds(out, path)

  # No return to save memory
  return(NULL)
}


