# Erstelle Datei f?r Baseline-Jahr f?r alle Kacheln ab 1917 (99 Jahre)
# Ein Besseres Startjahr bzgl. Kacheln w?re 1957 gewesen, aber dann ist der Zeitraum bis 2016 zu kurz. 1916 (100 Jahre) wurde nicht gew?hlt, weil es ein Schaltjahr ist.
# Baseline-Jahr: Pro Kachel ?ber die ersten 30 Jahre den Durschnitt der Tagesmessungen bilden. Aggregieren der Stationen zu Kacheln und aggregieren von 30 Jahren zu einem.

source("init.r")
#source("tile.r")
source("station.r")
source("weather_data.r")

init()

sdf_stations <- read_stations(path_stations)

year_start <- 1917
year_end <- year_start + 30 - 1
# Write CSV file with tile_id and year
path_baselines <- "R:\\Big Data Prak\\baselines\\weather_per_tile"
path_tiles_per_year = "R:\\Big Data Prak\\data_analyses\\tiles_per_year.csv"


# Create initial tiles file

# write list of all active tiles per year
for(i in (year_start:year_end)) {
  sdf_weather_data <- read_weather_data_org(path_weather_yearly, i)
  sdf_weather_data <- inner_join(sdf_weather_data, sdf_stations, by=c("Station" = "Id"))
  
  sdf_tiles_per_year <- sdf_weather_data %>% group_by(Tile_Id)  %>% summarise() %>% mutate(Year = as.integer(i))
  
  write.table(sdf_tiles_per_year, path_tiles_per_year, append={if (i != year_start) TRUE else FALSE}, na='', quote=TRUE, sep=",", col.names={i == year_start}, row.names=FALSE)
  print(i)
}

# read list of all active tiles per year
sdf_tiles_per_year <- spark_read_csv(sc, "tiles_per_year", 
               path = path_tiles_per_year, 
               header = TRUE, 
               infer_schema = TRUE)
# select initial tiles (tiles that are active in start year)
sdf_tiles_initial <- sdf_tiles_per_year %>% filter(Year == year_start) %>% select(Tile_Id)
# inner join to reject all tiles that were not active in start year and all tiles that had no data in >20 % of the time period
sdf_tiles_initial <- inner_join(sdf_tiles_per_year, sdf_tiles_initial, by=c("Tile_Id" = "Tile_Id")) %>% 
  group_by(Tile_Id) %>% summarise(Count=n()) %>% filter(Count/30 >= 0.8) %>% select(Tile_Id)
write.table(sdf_tiles_initial, path_tiles_initial, append=FALSE, na='', quote=TRUE, sep=",", col.names=TRUE, row.names=FALSE)


# Create baseline file

# read list of filtered tiles (tiles we consider) (initial tiles)
sdf_tiles_initial <- read_tiles_initial(path_tiles_initial)

# filter weather data by initial tiles and create one big file
for(i in (year_start:year_end)) {
  # reject data from unconsidered tiles (join and filter weather data)
  sdf_weather_data <- read_weather_data_org(path_weather_yearly, i)
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
  sdf_weather_sums_per_tile <- sdf_weather_data %>% filter(Element %in% c("WT02", "WT03", "WT04", "WT05", "WT07", "WT10", "WT11", "WT16", "WT17", "WT18")) %>% group_by(Date, Element, Tile_Id, Station_count)  %>% summarise(Value = n()/Station_count) %>% mutate(Element = "WTXX") %>% select(-Station_count)
  # reunite both
  sdf_weather_data <- union_all(sdf_weather_means_per_tile, sdf_weather_sums_per_tile)
  
  spark_write_csv(sdf_weather_data, path_baselines, mode={if (i != year_start) "append" else "overwrite"})
  
  print(i)
}

# Mean over all 30 years
sdf_weather_per_tile <- spark_read_csv(sc, "weather_per_tile", paste(path_baselines, "\\*", sep = ""), infer_schema = FALSE,
                                       columns = list(
                                         Date = "character",
                                         Element = "character",
                                         Tile_Id = "character",
                                         Value = "numeric")
                                       )
sdf_weather_baseline <- sdf_weather_per_tile %>% group_by(Date, Element, Tile_Id)  %>% summarise(Value = mean(Value))
spark_write_csv(sdf_weather_baseline, paste(path_processed, "baseline", sep = ""), mode = "overwrite")
