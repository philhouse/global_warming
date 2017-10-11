source("weather_data.r")
source("initial_tiles.r")
source("generalization.r")

# Generates the weather baseline year.
# The table is written as csv and contains daily, averaged (over given year span) weather measurements for each tile.
# During the excecution multiple temporary files are written and read again,
# that way one can manually continue the script if the livelong execution ran into an error at some point.
#
# All tiles are checked to meet a certain threshold of measurement coverage.
# (See initial_tiles.r\get_initial_tiles())
# Only the records that meet the criteria will be part of the baseline data set.
# 
# Input:
# path_weather_files:  Source path to the yearly CSV Files of the GHCN Daily data set
# sdf_tiled_stations: Spark data frame of stations file with added Tile_Id
# path_tmp_files: Target path to store temporary data
# path_target: Target path to store the generated spark data frame (sparklyr CSV files).
# year_start_baseline:  Calendar year to mark the start of the baseline generation
# year_span_baseline: Number of years to define the span used for the baseline generation
# measurement_coverage_threshold:  Decimal number of range [0..1].
#   Defines during the baseline generation which tiles should be used,
#   because they have enough measurements to cover at least 
#   (measurement_coverage_threshold * 100) % of the baseline span.
generate_tiled_weather_baseline = function( 
  path_weather_files, 
  sdf_tiled_stations, 
  path_tmp_files, 
  path_target,
  year_start_baseline, 
  year_span_baseline, 
  measurement_coverage_threshold)
{
  print( "Generating baseline for weather data ...")
  
  year_end_baseline <- year_start_baseline + year_span_baseline - 1
  path_tmp_tiles_yearly <- paste0( 
    path_tmp_files, 
    "tiles_yearly")
  path_tmp_tiled_weather_data_yearly <- paste0( 
    path_tmp_files, 
    "tiled_weather_data_yearly")
  
  sdf_tiles_initial <- get_initial_tiles( 
    path_weather_files = path_weather_files, 
    sdf_tiled_stations = sdf_tiled_stations, 
    path_tmp_tiles_yearly = path_tmp_tiles_yearly, 
    year_start = year_start_baseline, 
    year_span = year_span_baseline, 
    measurement_coverage_threshold = measurement_coverage_threshold)
  # Write one big file of weather data with records filtered for initial tiles
  # and measurement values generalized from stations to tiles
  write_filtered_data(
    path_weather_files = path_weather_files, 
    path_target = path_tmp_tiled_weather_data_yearly, 
    sdf_tiled_stations = sdf_tiled_stations, 
    sdf_tiles_initial = sdf_tiles_initial, 
    year_start_baseline = year_start_baseline, 
    year_end_baseline = year_end_baseline)
  
  # Calculate daily measurement values using mean over all 30 years
  print( "Calculating the means for the baseline year.")
  sdf_tiled_weather_baseline <- 
    read_tiled_weather_baseline( 
      path = paste0( path_tmp_tiled_weather_data_yearly, "\\*")) %>%
    mutate(
      Date = substring( Date, 5, 8)) %>%
    group_by(
      Date, 
      Element, 
      Tile_Id)  %>% 
    summarise(
      Value = mean(Value))
  print( "Writing baseline year.")
  sdf_tiled_weather_baseline
  spark_write_csv(
    sdf_tiled_weather_baseline, 
    path_target, 
    mode = "overwrite")
  print( "... Finished generating baseline for weather data.")
  return( sdf_tiled_weather_baseline)
}

# Writes one big file of filtered and generalized weather data.
# That means unconsidered tiles (non-initial tiles) are rejected and 
# weather measurements are generalized from stations to tiles.
# 
# Input:
# path_weather_files:  Source path to the yearly CSV Files of the GHCN Daily data set
# path_target: Target path to store the generated spark data frame (sparklyr CSV files).
# sdf_tiled_stations: Spark data frame of stations file with added Tile_Id
# sdf_tiles_initial: Spark data frame containing all tiles that should be used.
# year_start_baseline:  Calendar year to mark the start of the baseline generation
# year_end_baseline: Calendar year to mark the end (last year) of the baseline generation
write_filtered_data = function(
  path_weather_files, 
  path_target, 
  sdf_tiled_stations, 
  sdf_tiles_initial, 
  year_start_baseline, 
  year_end_baseline)
{
  print( paste0("Generalizing data from stations to tiles (", year_start_baseline, "-", year_end_baseline ,"). ..."))
  write_mode <- "overwrite"
  for(i in (year_start_baseline:year_end_baseline)) {
    # reject data from unconsidered tiles
    sdf_weather_data <- 
      read_weather_data_org_with_tile_id( 
        path_weather_files, 
        i, 
        sdf_tiled_stations) %>%
      limit_data_to_considered_tiles(
        sdf_considered_tiles = sdf_tiles_initial
      )
    
    sdf_tiled_weather_data <- 
      generalize_from_stations_to_tiles( 
        sdf_weather_data) %>% 
      # reorder columns to fit forced schema of read_tiled_weather_baseline function (If not forced Date will be imported as integer)
      select( 
        Date, 
        Element, 
        Tile_Id, 
        Value)
    
    spark_write_csv(
      sdf_tiled_weather_data, 
      path_target, 
      mode = write_mode)
    write_mode <- "append"
    print( paste0( "Wrote temporary weather data for year ", i, "."))
  }
  print( "... Finished generalizing data from stations to tiles.")
}