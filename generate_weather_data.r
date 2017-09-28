transform_for_output = function (
  sdf_tiled_weather, 
  sdf_tiled_stations,
  year) 
{
  sdf_tiled_weather %>%
    transform_to_wide_table() %>%
    left_join(
      sdf_tiled_stations %>% 
        group_by(Tile_Id, Lat, Long) %>%
        summarize(), 
      by = c("Tile_Id")) %>% 
    select(-Tile_Id) %>%
    mutate(Year = year)
}

transform_to_wide_table = function (
  list_sdf) 
{
  list_sdf <- 
    sapply(
      names(list_sdf), 
      lspread_weather_elements, 
      list_sdf_weather_data = list_sdf,
      simplify = FALSE,
      USE.NAMES = TRUE)
  
  sdf <- full_join_list(
    list_sdf,
    by = "Tile_Id"
  )
}

full_join_list = function(
  list_sdf, 
  by)
{
  sdf <- list_sdf[[1]]
  if ( length( list_sdf) >= 2) {
    for(i in 2:length(list_sdf)) {
      sdf <- 
        full_join(
          sdf,
          list_sdf[[i]],
          by = by)
    }
  }
  sdf
}

# Transform data for export (spread the key-value pair (Element, Value) across multiple Element columns)
# Workaround for bug with sparklyr's lazy queries 
# (When joining two SparkDataFrames, duplicate column names are renamed, 
# but one can not work with the new column names since dplyr does not know them yet due to the lazy querie behavior)
# workaround: converted from sdf to df, rename and convert back to sdf
lspread_weather_elements = function(
  name_list_element, 
  list_sdf_weather_data)
{
  print(paste("Transforming list element:", name_list_element))
  list_sdf_weather_data[[name_list_element]] <- 
    list_sdf_weather_data[[name_list_element]] %>% 
    collect() %>% 
    spread( Element, Value) %>% 
    rename(
      !!paste0( "PRCP_", name_list_element) := PRCP, 
      !!paste0( "WTXX_", name_list_element) := WTXX, 
      !!paste0( "TMAX_", name_list_element) := TMAX) %>% 
    copy_to(dest = sc, name = paste0('weather_data_', name_list_element), overwrite = TRUE)
}

generate_weather_data = function(
  path_weather_yearly_org,
  path_weather_data,
  path_stations_org,
  tile_size,
  path_tmp,
  year_start_baseline,
  year_span_baseline,
  year_start_data,
  year_end_data,
  baseline_measurement_coverage_threshold)
{
  # load functions
  source("init.r")
  source("weather_data.r")
  source("station.r")
  source("generate_baselines.r")
  source("generalization.r")
  source("initial_tiles.r")
  
  # Get paths, load packages and start spark connection
  init()
  path_tmp_baseline <- paste0( path_tmp, "baseline")
  
  sdf_tiled_stations <- 
    generate_tiled_stations_table( 
      path_source = path_stations_org, 
      tile_size = tile_size)
  
  # sdf_tiled_weather_baseline <-
  #   generate_tiled_weather_baseline(
  #     path_weather_files =
  #       path_weather_yearly_org,
  #     sdf_tiled_stations =
  #       sdf_tiled_stations,
  #     path_tmp_files =
  #       path_tmp,
  #     path_target =
  #       path_tmp_baseline,
  #     year_start_baseline =
  #       year_start_baseline,
  #     year_span_baseline =
  #       year_span_baseline,
  #     measurement_coverage_threshold =
  #       baseline_measurement_coverage_threshold)
  sdf_tiled_weather_baseline <- read_tiled_weather_baseline( path_tmp_baseline)
  
  sdf_tiled_weather_baseline <- 
    rename(
      sdf_tiled_weather_baseline, 
      Value_baseline = Value)
  
  sdf_tiles_initial <- 
    sdf_tiled_weather_baseline %>% 
    group_by(Tile_Id) %>% 
    summarise()
  
  print( paste0( "Generating weather data (", 
                 year_start_data, "-", year_end_data, 
                 ") ..."))
  write_mode <- "overwrite"
  for(i in (year_start_data:year_end_data)) {
    sdf_weather_data <- 
      read_weather_data_org_with_tile_id( 
        path_weather_yearly_org, 
        i, 
        sdf_tiled_stations) %>%
      limit_data_to_considered_tiles( 
        considered_tiles = sdf_tiles_initial) %>% 
      generalize_from_stations_to_tiles_and_calc_baseline_differences(
        sdf_tiled_weather_baseline) %>%
      generalize_to_list_of_time_segments() %>%
      transform_for_output(
        sdf_tiled_stations,
        year = i
      )
    
    spark_write_csv(sdf_weather_data, path_weather_data, mode = write_mode)
    write_mode <- "append"
    print( paste0( "Wrote weather data for year ", i, "."))
  }
  print( "... Finished generating weather data.") 
}