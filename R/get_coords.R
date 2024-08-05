get_coords <- function(ilon, lat){

  df_cwd <- readRDS(paste0("/data_1/CMIP6/tidy/cwd/cwd_", ilon, ".rds"))
  df_pcwd <- readRDS(paste0("/data_1/CMIP6/tidy/pcwd/pcwd_", ilon, ".rds"))

  df_cwd <- df_cwd |> tidyr::unnest(data)
  df_pcwd <- df_pcwd |> tidyr::unnest(data)

  df_cwd <- df_cwd |>
    rename(deficit_cwd = "deficit")

  df_pcwd <- df_pcwd |>
    rename(deficit_pcwd = "deficit")

  combined_df <- df_cwd |>
    left_join(df_pcwd, by = join_by(lon, lat, time))

  start <- lat
  end <- lat+1
  combined_df <- combined_df[(combined_df$lat >= start) & (combined_df$lat <= end), ]

  return (combined_df)

}
