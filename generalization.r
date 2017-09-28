# 
generalize_to_list_of_time_segments = function(
  sdf_tiled_weather_data)
{
  midterm_boundary_1 <- "0504" # %MM%DD
  midterm_boundary_2 <- "1103"
  list_tiled_weather_by_time_segment <- 
    list()
  
  list_tiled_weather_by_time_segment$year <- 
    sdf_tiled_weather_data %>% 
    generalize_by_time()
  list_tiled_weather_by_time_segment$summer <- 
    sdf_tiled_weather_data %>% 
    filter(
      Date > midterm_boundary_1 - 1 & Date < midterm_boundary_2) %>% 
    generalize_by_time()
  list_tiled_weather_by_time_segment$winter <- 
    sdf_tiled_weather_data %>% 
    filter(
      Date < midterm_boundary_1 | Date > midterm_boundary_2 - 1) %>% 
    generalize_by_time()
  
  list_tiled_weather_by_time_segment
}

generalize_by_time = function(
  sdf_tiled_weather_data)
{
  sdf_tiled_weather_data %>% 
    group_by( 
      Element, 
      Tile_Id) %>% 
    summarise(
      Value = mean(Value))
}

generalize_from_stations_to_tiles = function( 
  sdf_weather_data)
{
  # add station count per Tile_Id (used to normalize storm counts per tile)
  sdf_weather_data <- 
    sdf_weather_data %>% 
    add_station_count_per_tile()
  # calculate means per tile (temperatur data, precipitation data)
  sdf_weather_value_means_per_tile <- 
    sdf_weather_data %>% 
    filter(
      Element %in% c("PRCP", "TMAX")) %>% 
    group_by(
      Date, 
      Element, 
      Tile_Id)  %>% 
    summarise(
      Value = mean(Value))
  # calculate means per tile (stormy weather types)
  sdf_weather_occurrence_means_per_tile <- 
    sdf_weather_data %>% 
    filter(
      !Element %in% c("PRCP", "TMAX")) %>% 
    group_by(
      Date, 
      Tile_Id, 
      Station_count)  %>% 
    summarise(
      Value = n()/Station_count) %>% 
    select(
      -Station_count) %>%
    mutate(
      Element = "WTXX")
  # reunite both
  sdf_tiled_weather_data <- 
    union_all(
      sdf_weather_value_means_per_tile, 
      sdf_weather_occurrence_means_per_tile)
}

# The function could not be splitted in two due to a bug (See bug comment below)
generalize_from_stations_to_tiles_and_calc_baseline_differences = function( 
  sdf_weather_data,
  sdf_tiled_weather_baseline)
{
  sdf_weather_data <- 
    sdf_weather_data %>% 
    add_station_count_per_tile() %>% 
    mutate(
      Date = substring(Date, 5, 8)) # filter %YY%MM%DD to %MM%DD for a group_by Date later
  
  # calculate means per tile (temperatur data, precipitation data)
  sdf_weather_value_means_per_tile <- 
    sdf_weather_data %>% 
    filter(
      Element %in% c("PRCP", "TMAX")) %>% 
    group_by(
      Date, 
      Element, 
      Tile_Id)  %>% 
    summarise(
      Value = mean(Value)) %>%
    inner_join(
      sdf_tiled_weather_baseline,
      by = c("Date", "Element", "Tile_Id"))
  
  # calculate means per tile (stormy weather types)
  sdf_weather_occurrence_means_per_tile <- 
    sdf_weather_data %>% 
    filter(
      !Element %in% c("PRCP", "TMAX")) %>% 
    group_by(
      Date, 
      Tile_Id, 
      Station_count)  %>% 
    summarise(
      Value = n()/Station_count) %>% 
    select(
      -Station_count) %>%
    mutate(
      Element = "WTXX") %>%
    inner_join(
      sdf_tiled_weather_baseline,
      by = c("Date", "Element", "Tile_Id"))
  
  # Reunite both
  # Bug: sparklyr seems to run idle when doing one single join after the union,
  # instead of the two seperate joins above before the union
  sdf_tiled_weather_data <- 
    union_all(
      sdf_weather_value_means_per_tile, 
      sdf_weather_occurrence_means_per_tile)
  
  # calculate baseline differences
  sdf_tiled_weather_data <- 
    sdf_tiled_weather_data %>% 
    mutate( 
      Value = Value - Value_baseline) %>% 
    select(
      -Value_baseline)
}