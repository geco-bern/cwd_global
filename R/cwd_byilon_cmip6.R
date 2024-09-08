#' @export

cwd_byilon <- function(
    ilon,
    indir_evspsbl,
    indir_prec,
    indir_tas,
    indir_rlus,
    indir_rlds,
    indir_rsds,
    indir_rsus,
    indir_elevation,
    outdir_cwd,
    outdir_pcwd,
    fileprefix_cwd,
    fileprefix_pcwd
    ){

  # load functions that will be applied to time series
  #' @importFrom here here
  source(paste0(here::here(), "/R/my_cwd.R"))
  source(paste0(here::here(), "/R/my_pcwd.R"))


  # read from file that contains tidy data for a single longitudinal band
  #' @importFrom readr read_rds

  # read evapotranspiration file tidy
  filnam <- list.files(
    indir_evspsbl,
    pattern = paste0("evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
    )
  df_evap <- readr::read_rds(filnam)


  # read precipitation file tidy
  filnam <- list.files(
    indir_prec,
    pattern = paste0("pr_day_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_prec <- readr::read_rds(filnam)


  # read temperature file tidy
  filnam <- list.files(
    indir_tas,
    pattern = paste0("tas_day_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_tas <- readr::read_rds(filnam)


  # read radiation files tidy
  filnam <- list.files(
    indir_rlus,
    pattern = paste0("rlus_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rlus <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rlds,
    pattern = paste0("rlds_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rlds <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rsds,
    pattern = paste0("rsds_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rsds <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rsus,
    pattern = paste0("rsus_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rsus <- readr::read_rds(filnam)


  # read elevation file and convert to data frame
  library(terra)
  filnam <- list.files(
    indir_elevation,
    pattern = paste0("elevation.nc"),
    full.names = TRUE
  )

  rasta_elevation <- terra::rast(filnam)

  ## read the needed longitude value and extract the latitude values
  lon <- df_prec[["lon"]][1]
  latitudes <- unique(df_prec$lat)

  ## create a data frame of the coordinates
  loc <- data.frame(lon = rep(lon, length(latitudes)), lat = latitudes)

  ## convert to vector points
  points <- terra::vect(loc, geom = c("lon", "lat"), crs = "EPSG:4326")

  ## extract values
  vals <- terra::extract(rasta_elevation, points, xy = FALSE, ID = FALSE, method = "simple")

  # combine coordinates with extracted values
  df_elevation <- data.frame(
    lon = lon,
    lat = latitudes,
    value = vals
  )

  ## set NA-value at 90 degrees to 0
  df_elevation[is.na(df_elevation)] <- 0


  # unnest all the data frames
  #' @importFrom tidyr unnest
  df_rsds <- df_rsds |> tidyr::unnest(data)
  df_rsus <- df_rsus |> tidyr::unnest(data)
  df_rlds <- df_rlds |> tidyr::unnest(data)
  df_rlus <- df_rlus |> tidyr::unnest(data)
  df_evap <- df_evap |> tidyr::unnest(data)
  df_prec <- df_prec |> tidyr::unnest(data) # lon lat pr time
  df_tas <- df_tas |> tidyr::unnest(data)


  # data wrangling and resolution adjustments

  ## extract year and month from the time column
  df_prec <- df_prec |>
    mutate(year = lubridate::year(time), month = lubridate::month(time))

  df_evap <- df_evap |>
    mutate(year = lubridate::year(time), month = lubridate::month(time))|>
    select(-time)

  ## cwd
  ### merge all such that monthly data is repeated for each day within month
  df_cwd <- df_prec |>
    left_join(df_evap, by = join_by(lon, lat, year, month)) |>
    left_join(df_tas, by = join_by(lon, lat, time))|>
    dplyr::select(-year, -month)

  ## compute net_radiation
  ### create new data frame
  df_radiation <- df_rsds |>
    left_join(df_rsus, by = join_by(lon, lat, time))|>
    left_join(df_rlds, by = join_by(lon, lat, time))|>
    left_join(df_rlus, by = join_by(lon, lat, time))

  ### calculate net radiation
  df_net_radiation <- df_radiation |>
    mutate(net_radiation = (rsds - rsus) + (rlds - rlus))|>
    select(-rsds, -rsus, -rlds, -rlus)

  ## extract year and month from the time column
  df_net_radiation <- df_net_radiation |>
    mutate(year = lubridate::year(time), month = lubridate::month(time))|>
    select(-time)

  ## pcwd
  ### merge all such that monthly data is repeated for each day within month
  df_pcwd <- df_prec |>  # one of the daily data frames
    left_join(df_net_radiation, by = join_by(lon, lat, year, month))|>
    left_join(df_tas, by = join_by(lon, lat, time))|>
    left_join(df_elevation, by = join_by(lon, lat))|>
    dplyr::select(-year, -month)

  df_pcwd <- as_tibble(df_pcwd)


  # out_cwd
  out_cwd <- df_cwd |>

    # group data by grid cells and wrap time series for each grid cell into a new
    # column, by default called 'data'.
    dplyr::group_by(lon, lat) |>
    #' @importFrom tidyr nest
    tidyr::nest() |>

    # apply the custom function on the time series data frame separately for
    # each grid cell.
    #' @importFrom purrr map
    mutate(data = purrr::map(data, ~my_cwd(.)))


  # out pcwd
  out_pcwd <- df_pcwd |>

    # group data by grid cells and wrap time series for each grid cell into a new
    # column, by default called 'data'.
    group_by(lon, lat) |>
    tidyr::nest() |>

    # apply the custom function on the time series data frame separately for
    # each grid cell.
    mutate(data = purrr::map(data, ~my_pcwd(.)))


  # write (complemented) data to cwd-file with meaningful name and index counter
  path_cwd <- paste0(outdir_cwd, "/", fileprefix_cwd, "_", ilon, ".rds")
  message(
    paste0(
      "Writing file ", path_cwd , " ..."
    )
  )
  #' @importFrom readr write_rds
  readr::write_rds(
    out_cwd,
    path_cwd
    )


  # write (complemented) data to pcwd-file.
  path_pcwd <- paste0(outdir_pcwd, "/", fileprefix_pcwd, "_", ilon, ".rds")
  message(
    paste0(
      "Writing file ", path_pcwd, " ..."
    )
  )
  readr::write_rds(
    out_pcwd,
    path_pcwd
  )

  # don't return data - it's written to file
}
