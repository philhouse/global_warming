# Reads the csv containing the weather baseline data per tile
read_tiled_weather_baseline = function (
  path) 
{
  sdf_tiled_weather_baseline <- spark_read_csv(
    sc, 
    "tiled_weather_baseline", 
    path, 
    infer_schema = FALSE,
     columns = list(
       Date = "character",
       Element = "character",
       Tile_Id = "character",
       Value = "numeric"))
}

# Reads the csv containing the original weather data of one year with added tile ID.
# Also filters the records to only contain measurements for:
# "PRCP", "TMAX", "WT02", "WT03", "WT04", "WT05", "WT07", "WT10", "WT11", "WT16", "WT17", "WT18"
# (That is precipitation, daily maximum temperature and some stormy weather types)
#
# Input:
# path: Source path to the folder containing the yearly CSV files.
# year: Calendar year naming the CSV file
# sdf_tiled_stations: Spark data frame of stations table with added Tile_Id
read_weather_data_org_with_tile_id = function (
  path, 
  year, 
  sdf_tiled_stations)
{
  sdf_weather_data <- spark_read_csv(
    sc, 
    "weather_data_org", 
    path = paste0(
      path, 
      year,
      ".csv"), 
    header = FALSE, 
    infer_schema = FALSE,
    columns = list(
      Station = "character",
      Date = "character",
      Element = "character",
      Value = "integer",
      MFlag = "character",
      QFlag = "character",
      SFlag = "character",
      Time = "character"))
  
  weather_elements <- c(
    "PRCP", 
    "TMAX", 
    "WT02", "WT03", "WT04", "WT05", "WT07", "WT10", "WT11", "WT16", "WT17", "WT18")
  sdf_weather_data <- 
    sdf_weather_data %>% 
      filter(
        is.null(QFlag) && 
        Element %in% weather_elements) %>% 
      select(
        Station, 
        Date, 
        Element, 
        Value)
  
  sdf_weather_data <- 
    inner_join( 
      sdf_weather_data, 
      sdf_tiled_stations, 
      by = c("Station" = "Id"))
  
  return (sdf_weather_data)
}