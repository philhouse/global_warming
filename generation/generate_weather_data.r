source("init.r")
source("weather_data.r")
source("station.r")

init()

sdf_weather_baseline <- read_weather_baseline(path_baseline)
sdf_tiles_initial <- read_tiles_initial(path_tiles_initial)
sdf_stations <- read_stations(path_stations)

year_start <- 1917
year_end <- 2016

# change Title_Id to tile center lat and long
sdf_stations_tile_center <- read_stations_org(path_stations_org)
# calculate tile center
sdf_stations_tile_center <- sdf_stations_tile_center %>% mutate(
  Lat = as.integer((Lat + 90) %/% tile_size) * tile_size - 90 + 5,
  Long = as.integer((Long + 180) %/% tile_size) * tile_size - 180 + 5
)
sdf_stations_tile_center <- inner_join(sdf_stations, sdf_stations_tile_center, by ="Id") %>% group_by(Tile_Id, Lat, Long) %>% summarise()


for(i in (year_start:year_end)) {
  # reject data from unconsidered tiles (join and filter weather data)
  sdf_weather_data <- read_weather_data_org(path_weather_yearly_org, i)
  sdf_weather_data <- inner_join(sdf_weather_data, sdf_stations, by=c("Station" = "Id"))
  sdf_weather_data <- inner_join(sdf_tiles_initial, sdf_weather_data, by = "Tile_Id") # filter all data records that belong to an unconsidered tile
  
  # generalize data from stations to tiles
  
  # preprocess weather data
  sdf_weather_data <- sdf_weather_data %>% mutate(Date = substring(Date, 5, 8)) # filter %YY%MM%DD to %MM%DD for the group_by later
  # add station count per tile id of this year (used to normalize storm counts per tile)
  sdf_station_count_per_tile <- sdf_weather_data %>% group_by(Station, Tile_Id) %>% summarise() %>% group_by(Tile_Id) %>% summarise(Station_count = n()) %>% select(Tile_Id, Station_count)
  sdf_weather_data <- inner_join(sdf_station_count_per_tile, sdf_weather_data, by = "Tile_Id") %>% select(-Station)
  
  # calculate means per tile (temperatur data, precipitation data)
  sdf_weather_means_per_tile <- sdf_weather_data %>% filter(Element %in% c("PRCP", "TMAX")) %>% group_by(Date, Element, Tile_Id)  %>% summarise(Value = mean(Value))
  # calculate normed sums per tile (stormy weather types)
  sdf_weather_sums_per_tile <- sdf_weather_data %>% filter(! Element %in% c("PRCP", "TMAX")) %>% group_by(Date, Element, Tile_Id, Station_count)  %>% summarise(Value = n()/Station_count) %>% mutate(Element = "WTXX") %>% select(-Station_count)
  
  # reunite both and calculate value differences with baseline
  sdf_weather_sums_per_tile <- inner_join(sdf_weather_sums_per_tile, sdf_weather_baseline, by = c("Date", "Element", "Tile_Id"))
  sdf_weather_means_per_tile <- inner_join(sdf_weather_means_per_tile, sdf_weather_baseline, by = c("Date", "Element", "Tile_Id"))
  sdf_weather_data <- union_all(sdf_weather_means_per_tile, sdf_weather_sums_per_tile)
  #sdf_weather_data <- inner_join(sdf_weather_data, sdf_weather_baseline, by = c("Date", "Element", "Tile_Id"))
  # rename() workaround because mutate couldn't handle the new column names .x, .y due to lazy querying
  sdf_weather_data <- rename(sdf_weather_data, Value = Value.x)
  sdf_weather_data <- rename(sdf_weather_data, Value_baseline = Value.y)
  sdf_weather_data <- sdf_weather_data %>% mutate( Value = Value - Value_baseline) %>% select(-Value_baseline)
  
  
  # generalize data by time
  
  # generalize from days to year
  sdf_weather_data_year <- sdf_weather_data %>% group_by(Element, Tile_Id) %>% summarise (Value = mean(Value))
  # generalize from days to summer time
  sdf_weather_data_summer <- sdf_weather_data %>% filter(Date > "0504", Date > "1103") %>% group_by(Element, Tile_Id) %>% summarise (Value = mean(Value))
  # generalize from days to winter time
  sdf_weather_data_winter <- sdf_weather_data %>% filter(Date < "0504", Date < "1103") %>% group_by(Element, Tile_Id) %>% summarise (Value = mean(Value))
  
  # Transform data for export
  # converted from sdf to df back to sdf due to error with sparklyr's lazy queries
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
  
  tmp <- left_join(df_weather_data_year, df_weather_data_summer, by = "Tile_Id")
  df_weather_data <- left_join(tmp, df_weather_data_winter, by = "Tile_Id")
  
  sdf_weather_data <- copy_to(sc, df_weather_data, name = 'weather_data', overwrite = TRUE)
  sdf_weather_data <- sdf_weather_data %>% mutate(Year = as.integer(i))
  
  # exchange Tile_Id by Lat and Long of tile center
  sdf_weather_data <- inner_join(sdf_weather_data, sdf_stations_tile_center) %>% select(-Tile_Id)
  
  spark_write_csv(sdf_weather_data, path_weather_data, mode={if (i != year_start) "append" else "overwrite"})
  
  print(i)
}

