# load functions
source("init.r")
source("weather_data.r")
source("station.r")
source("generate_baselines.r")

# Get paths, load packages and start spark connection
init()
path_baseline_tmp <- paste0( path_tmp, "baseline")

# Generate stations file with added tile information (id as well as latitude and longitude of tile center)
sdf_stations <- generate_tiled_stations_table( path_source = path_stations_org, 
                                               tile_size = tile_size)

# Generate baseline year for each tile
sdf_weather_baseline <- generate_weather_data_baseline(path_weather_files = path_weather_yearly_org, 
                                                       sdf_stations = sdf_stations,
                                                       path_tmp_files = path_tmp,
                                                       path_target = path_baseline_tmp,
                                                       year_start_baseline = year_start_baseline,
                                                       year_span_baseline = year_span_baseline,
                                                       measurement_coverage_threshold = baseline_measurement_coverage_threshold)
# You might want to run this line in case of a crash: 
#sdf_weather_baseline <- read_weather_baseline(path_baseline_tmp)

# Generate weather data

sdf_weather_baseline <- rename(sdf_weather_baseline, Value_baseline = Value)
sdf_tiles_initial <- sdf_weather_baseline %>% group_by(Tile_Id) %>% summarise() #read_tiles_initial(path_tiles_initial)

print( "Generating weather data ...")

write_mode <- "overwrite"
for(i in (year_start_data:year_end_data)) {
  # reject data from unconsidered tiles
  sdf_weather_data <- read_weather_data_org(path_weather_yearly_org, i)
  sdf_weather_data <- inner_join(sdf_weather_data, sdf_stations, by=c("Station" = "Id"))
  sdf_weather_data <- inner_join(sdf_tiles_initial, sdf_weather_data, by = "Tile_Id") # filter all data records that belong to an unconsidered tile

  # preprocessing
  sdf_weather_data <- sdf_weather_data %>% 
    mutate(Date = substring(Date, 5, 8))
  
  # generalize data from stations to tiles
  
  # add station count per tile id of this year (used to normalize storm counts per tile)
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
    #filter(Element != "WTXX") %>% 
    filter(Element %in% c("PRCP", "TMAX")) %>% 
    group_by(Date, Element, Tile_Id)  %>% 
    summarise(Value = mean(Value))
  # calculate means per tile (stormy weather types)
  sdf_weather_manual_means_per_tile <- sdf_weather_data %>% 
    #filter(Element == "WTXX") %>% 
    filter(! Element %in% c("PRCP", "TMAX")) %>% 
    group_by(Date, Tile_Id, Station_count)  %>% 
    summarise(Value = n()/Station_count) %>% 
    select(-Station_count) %>%
    mutate(Element = "WTXX")
  
  # reunite both
  sdf_weather_means_per_tile <- inner_join(sdf_weather_means_per_tile, sdf_weather_baseline, by = c("Date", "Element", "Tile_Id"))
  sdf_weather_manual_means_per_tile <- inner_join(sdf_weather_manual_means_per_tile, sdf_weather_baseline, by = c("Date", "Element", "Tile_Id"))
  sdf_weather_data <- union_all(sdf_weather_means_per_tile, sdf_weather_manual_means_per_tile)
  #sdf_weather_data <- inner_join(sdf_weather_data, sdf_weather_baseline, by = c("Date", "Element", "Tile_Id"))
  
  sdf_weather_data <- sdf_weather_data %>% 
    mutate( Value = Value - Value_baseline) %>% 
    select(-Value_baseline)
  
  
  # generalize data by time
  
  # generalize from days to year
  ##### DATE ALREADY %MM%DD ?
  sdf_weather_data_year <- sdf_weather_data %>% 
    group_by(Element, Tile_Id) %>% 
    summarise (Value = mean(Value))
  # generalize from days to summer time
  sdf_weather_data_summer <- sdf_weather_data %>% 
    filter(Date > "0504", Date > "1103") %>% 
    group_by(Element, Tile_Id) %>% 
    summarise (Value = mean(Value))
  # generalize from days to winter time
  sdf_weather_data_winter <- sdf_weather_data %>% 
    filter(Date < "0504", Date < "1103") %>% 
    group_by(Element, Tile_Id) %>% 
    summarise (Value = mean(Value))
  
  # Transform data for export (spread the key-value pair (Element, Value) across multiple Element columns)
  # Workaround for bug with sparklyr's lazy queries 
  # (When joining two SparkDataFrames, duplicate column names are renamed, 
  # but one can not work with the new column names since dplyr does not know them yet due to the lazy querie behavior)
  # workaround: converted from sdf to df, rename and convert back to sdf
  df_weather_data_year <- sdf_weather_data_year %>% collect() %>% spread(Element, Value)
  df_weather_data_summer <- sdf_weather_data_summer %>% collect() %>% spread(Element, Value)
  df_weather_data_winter <- sdf_weather_data_winter %>% collect() %>% spread(Element, Value)
  
  df_weather_data_year <- rename(df_weather_data_year, PRCP_year = PRCP)
  df_weather_data_year <- rename(df_weather_data_year, WTXX_year = WTXX)
  df_weather_data_year <- rename(df_weather_data_year, TMAX_year = TMAX)
  df_weather_data_summer <- rename(df_weather_data_summer, PRCP_summer = PRCP)
  df_weather_data_summer <- rename(df_weather_data_summer, WTXX_summer = WTXX)
  df_weather_data_summer <- rename(df_weather_data_summer, TMAX_summer = TMAX)
  df_weather_data_winter <- rename(df_weather_data_winter, PRCP_winter = PRCP)
  df_weather_data_winter <- rename(df_weather_data_winter, WTXX_winter = WTXX)
  df_weather_data_winter <- rename(df_weather_data_winter, TMAX_winter = TMAX)
  
  sdf_weather_data_year <- copy_to(sc, df_weather_data_year, name = 'weather_data_year', overwrite = TRUE)
  sdf_weather_data_summer <- copy_to(sc, df_weather_data_summer, name = 'weather_data_summer', overwrite = TRUE)
  sdf_weather_data_winter <- copy_to(sc, df_weather_data_winter, name = 'weather_data_winter', overwrite = TRUE)
  
  tmp <- left_join(sdf_weather_data_year, sdf_weather_data_summer, by = c("Tile_Id"))
  sdf_weather_data <- left_join(tmp, sdf_weather_data_winter, by = c("Tile_Id"))
  
  # exchange Tile_Id by Lat and Long of tile center
  sdf_weather_data <- inner_join(sdf_weather_data, sdf_stations %>% select(-Id), by = c("Tile_Id")) %>% 
    select(-Tile_Id)
  sdf_weather_data <- sdf_weather_data %>%
    mutate(Year = i)
  
  spark_write_csv(sdf_weather_data, path_weather_data, mode = write_mode)
  write_mode <- "append"
  print( paste0( "Wrote weather data for year ", i, "."))
}
print( "... Finished generating weather data.")