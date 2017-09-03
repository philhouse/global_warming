# Erstelle Datei f?r Baseline-Jahr f?r alle Kacheln ab 1917 (99 Jahre)
# Ein Besseres Startjahr bzgl. Kacheln w?re 1957 gewesen, aber dann ist der Zeitraum bis 2016 zu kurz. 1916 (100 Jahre) wurde nicht gew?hlt, weil es ein Schaltjahr ist.
# Baseline-Jahr: Pro Kachel ?ber die ersten 30 Jahre den Durschnitt der Tagesmessungen bilden. Aggregieren der Stationen zu Kacheln und aggregieren von 30 Jahren zu einem.

source("weather_data.r")
source("initial_tiles.r")

# generates weather baseline year
# During excecution multiple temporary files are written and read again. That way one can manually continue the script if the livelong excecution ran into an error at some point.
generate_weather_data_baseline = function( 
  path_weather_files, 
  sdf_stations, 
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
    sdf_stations = sdf_stations, 
    path_tmp_tiles_yearly = path_tmp_tiles_yearly, 
    year_start = year_start_baseline, 
    year_span = year_span_baseline, 
    measurement_coverage_threshold = measurement_coverage_threshold)
  #spark_write_csv(sdf_tiles_initial, paste0(path_tmp_files, "\\tiles_initial"), mode="overwrite")
  #sdf_tiles_initial <- spark_read_csv(sc, "tiles_initial", paste0(path_tmp_files, "\\tiles_initial"))
  write_filtered_data(
    path_weather_files = path_weather_files, 
    path_target = path_tmp_tiled_weather_data_yearly, 
    sdf_stations = sdf_stations, 
    sdf_tiles_initial = sdf_tiles_initial, 
    year_start_baseline = year_start_baseline, 
    year_end_baseline = year_end_baseline)
  
  # Mean over all 30 years
  print( "Calculating the means for the baseline year.")
  sdf_weather_per_tile <- read_weather_baseline( 
    path = paste0( 
      path_tmp_tiled_weather_data_yearly, 
      "\\*"))
  sdf_weather_baseline <- sdf_weather_per_tile %>% 
    group_by(
      Date, 
      Element, 
      Tile_Id)  %>% 
    summarise(
      Value = mean(Value))
  print( "Writing baseline year.")
  spark_write_csv(
    sdf_weather_baseline, 
    path_target, 
    mode = "overwrite")
  print( "... Finished generating baseline for weather data.")
  return( sdf_weather_baseline)
}


write_filtered_data = function(
  path_weather_files, 
  path_target, 
  sdf_stations, 
  sdf_tiles_initial, 
  year_start_baseline, 
  year_end_baseline)
{
  print( paste0("Generalizing data from stations to tiles (", year_start_baseline, "-", year_end_baseline ,"). ..."))
  # create one big temporary file of filtered and generalized weather data
  write_mode <- "overwrite"
  for(i in (year_start_baseline:year_end_baseline)) {
    
    # reject data from unconsidered tiles
    sdf_weather_data <- read_weather_data_org(path_weather_files, i)
    sdf_weather_data <- inner_join(sdf_weather_data, sdf_stations, by=c("Station" = "Id"))
    sdf_weather_data <- inner_join(sdf_tiles_initial, sdf_weather_data, by = "Tile_Id")
    
    sdf_weather_data <- generalize_from_stations_to_tiles(sdf_weather_data) %>% 
      mutate(Date = substring(Date, 5, 8)) %>%
      select(Date, Element, Tile_Id, Value) #select is necessary to match the column order with the forced schema of read_weather_baseline function. If not forced Date will be imported as integer.
    
    spark_write_csv(sdf_weather_data, 
                    path_target, 
                    mode = write_mode)
    write_mode <- "append"
    print( paste0( "Wrote temporary weather data for year ", i, "."))
  }
  print( "... Finished generalizing data from stations to tiles.")
}

# generalize data from stations to tiles
generalize_from_stations_to_tiles = function( 
  sdf_weather_data)
{
  # add station count per Tile_Id (used to normalize storm counts per tile)
  sdf_station_count_per_tile <- sdf_weather_data %>% 
    group_by(Station, Tile_Id) %>% 
    summarise() %>% 
    group_by(Tile_Id) %>% 
    summarise(Station_count = n()) %>% 
    select(Tile_Id, Station_count)
  sdf_weather_data <- inner_join(sdf_station_count_per_tile, sdf_weather_data, by = "Tile_Id") %>% 
    select(-Station)
  
  # calculate means per tile (temperatur data, precipitation data)
  sdf_weather_means_per_tile <- sdf_weather_data %>% 
    filter(Element %in% c("PRCP", "TMAX")) %>% 
    group_by(Date, Element, Tile_Id)  %>% 
    summarise(Value = mean(Value))
  # calculate means per tile (stormy weather types)
  sdf_weather_manual_means_per_tile <- sdf_weather_data %>% 
    #ToDo: Mutate angewenden, falls nicht schon bei read_weather_data_org enthalten. Bei mutate auf WTXX muss auch summarise(Value = sum(Value)) erfolgen.
    filter(!Element %in% c("PRCP", "TMAX")) %>% 
    group_by(Date, Tile_Id, Station_count)  %>% 
    summarise(Value = n()/Station_count) %>% 
    select(-Station_count) %>% 
    mutate(Element = "WTXX")
  # reunite both
  sdf_weather_data <- union_all(sdf_weather_means_per_tile, sdf_weather_manual_means_per_tile)
}