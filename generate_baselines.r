# Erstelle Datei f?r Baseline-Jahr f?r alle Kacheln ab 1917 (99 Jahre)
# Ein Besseres Startjahr bzgl. Kacheln w?re 1957 gewesen, aber dann ist der Zeitraum bis 2016 zu kurz. 1916 (100 Jahre) wurde nicht gew?hlt, weil es ein Schaltjahr ist.
# Baseline-Jahr: Pro Kachel ?ber die ersten 30 Jahre den Durschnitt der Tagesmessungen bilden. Aggregieren der Stationen zu Kacheln und aggregieren von 30 Jahren zu einem.

source("weather_data.r")
source("initial_tiles.r")
source("generalization.r")

# generates weather baseline year
# During excecution multiple temporary files are written and read again. That way one can manually continue the script if the livelong excecution ran into an error at some point.
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
  #spark_write_csv(sdf_tiles_initial, paste0(path_tmp_files, "\\tiles_initial"), mode="overwrite")
  #sdf_tiles_initial <- spark_read_csv(sc, "tiles_initial", paste0(path_tmp_files, "\\tiles_initial"))
  write_filtered_data(
    path_weather_files = path_weather_files, 
    path_target = path_tmp_tiled_weather_data_yearly, 
    sdf_tiled_stations = sdf_tiled_stations, 
    sdf_tiles_initial = sdf_tiles_initial, 
    year_start_baseline = year_start_baseline, 
    year_end_baseline = year_end_baseline)
  
  # Mean over all 30 years
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
  spark_write_csv(
    sdf_tiled_weather_baseline, 
    path_target, 
    mode = "overwrite")
  print( "... Finished generating baseline for weather data.")
  return( sdf_tiled_weather_baseline)
}

write_filtered_data = function(
  path_weather_files, 
  path_target, 
  sdf_tiled_stations, 
  sdf_tiles_initial, 
  year_start_baseline, 
  year_end_baseline)
{
  print( paste0("Generalizing data from stations to tiles (", year_start_baseline, "-", year_end_baseline ,"). ..."))
  # create one big temporary file of filtered and generalized weather data
  write_mode <- "overwrite"
  for(i in (year_start_baseline:year_end_baseline)) {
    # reject data from unconsidered tiles
    sdf_weather_data <- read_weather_data_org_with_tile_id( 
      path_weather_files, 
      i, 
      sdf_tiled_stations)
    limit_data_to_considered_tiles(
      data = sdf_weather_data, 
      considered_tiles = sdf_tiles_initial
    )
    
    sdf_weather_data <- preprocessing( sdf_weather_data)
    
    sdf_tiled_weather_data <- 
      generalize_from_stations_to_tiles( 
        sdf_weather_data) %>% 
      # reorder columns to fit forced schema of read_weather_baseline function (If not forced Date will be imported as integer)
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