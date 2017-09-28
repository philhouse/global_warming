read_tiles_initial = function (
  path) 
{
  sdf_tiles_initial <- spark_read_csv(
    sc, 
    "tiles_initial", 
    path = path, 
    header = TRUE, 
    infer_schema = TRUE)
}

# returns initial tiles, 
# that is tiles which are active since the first year and 
# have enough data to cover* the time span properly.
# *(defined by measurement_coverage_threshold)
get_initial_tiles = function(
  path_weather_files, 
  sdf_tiled_stations, 
  path_tmp_tiles_yearly, 
  year_start, 
  year_span, 
  measurement_coverage_threshold) 
{
  year_end <- year_start + year_span - 1
  print( paste0(
    "Determining initial tiles (", 
    year_start, 
    "-", 
    year_end ,
    "). ..."))
  
  # write (and read) list of all active tiles per year
  append <- FALSE
  for(i in (year_start:year_end)) 
  {
    sdf_weather_data <- read_weather_data_org_with_tile_id( 
      path_weather_files, 
      i,
      sdf_tiled_stations)
    
    sdf_tiles_per_year <- get_active_tiles_of_year(
      sdf_weather_data, 
      year_span = year_span, 
      measurement_coverage_threshold = measurement_coverage_threshold)
    
    spark_write_csv(
      sdf_tiles_per_year, 
      path = path_tmp_tiles_yearly, 
      mode = {
        if (append) "append" 
        else "overwrite"}) 
    append <- TRUE
    print( paste0( 
      "Wrote temporary data (Active tiles) for year ", 
      i, 
      "."))
  }
  sdf_tiles_per_year <- spark_read_csv(
    sc, 
    "tmp", 
    path_tmp_tiles_yearly)
  
  # filter tiles that are active in start year
  sdf_tiles_initial <- sdf_tiles_per_year %>% 
    filter(Year == year_start) %>% 
    select(Tile_Id)
  sdf_tiles_initial <- inner_join(
    sdf_tiles_per_year, 
    sdf_tiles_initial, 
    by=c("Tile_Id"))
  sdf_tiles_initial <- get_active_tiles_of_years(
    sdf_tiles_initial, 
    year_span = year_span, 
    measurement_coverage_threshold = measurement_coverage_threshold)
  print( "... Finished determining initial tiles.")
  sdf_tiles_initial
}

# filter for coverage of the year (tiles that have measurement records of at least 80 % of the year)
get_active_tiles_of_year = function(
  sdf_weather_data, 
  year_span, 
  measurement_coverage_threshold)
{
  sdf_weather_data %>% 
    # summarise to one record per tile per day
    group_by(Date, Tile_Id) %>% 
    summarise() %>% 
    # count those records for each tile
    mutate(Year = substr(Date, 1, 4)) %>% 
    group_by(Tile_Id, Year)  %>% 
    summarise(Count = n()) %>%
    # filter tiles
    filter(
      Count / year_span >= 
      measurement_coverage_threshold) %>% 
    select(Tile_Id, Year)
}

# filter for coverage of the baseline time span (tiles that have measurement records of at least 80 % of the years)
get_active_tiles_of_years = function(
  sdf_weather_data, 
  year_span, 
  measurement_coverage_threshold)
{
  sdf_weather_data %>%
    group_by(Tile_Id) %>% 
    summarise(Count=n()) %>% 
    filter(Count/year_span >= measurement_coverage_threshold) %>% 
    select(Tile_Id)
}

limit_data_to_considered_tiles = function(
  data, 
  considered_tiles)
{
  inner_join( 
    data, 
    considered_tiles, 
    by = "Tile_Id")
}

add_station_count_per_tile = function(
  sdf_tiled_weather_data)
{
  sdf_tiled_weather_data %>% 
    group_by(
      Station, 
      Tile_Id) %>% 
    summarise() %>% 
    group_by(
      Tile_Id) %>% 
    summarise(
      Station_count = n()) %>% 
    select(
      Tile_Id, 
      Station_count) %>%
    inner_join(
      sdf_tiled_weather_data, 
      by = "Tile_Id") %>% 
    select(
      -Station)
}