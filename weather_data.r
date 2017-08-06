read_weather_baseline = function (path) {
  sdf_weather_baseline <- spark_read_csv(sc, "weather_baseline", path, infer_schema = FALSE,
                                         columns = list(
                                           Date = "character",
                                           Element = "character",
                                           Tile_Id = "character",
                                           Value = "numeric")
  )
  #sdf_weather_baseline %>% filter(! Element == "SNOW") #ToDo: Rewrite baseline file
}

# ToDo: Move this to tile.r?
read_tiles_initial = function (path) {
  sdf_tiles_initial <- spark_read_csv(sc, "tiles_initial", 
                                      path = path, 
                                      header = TRUE, 
                                      infer_schema = TRUE)
}

read_weather_data_org = function (path, year) {
  sdf_weather_data <- spark_read_csv(sc, "weather_data_org", 
                                     path = paste(path, year,".csv", sep = ""), 
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
                                       Time = "character"
                                     )
  )
  weather_elements <- c("PRCP", 
                        "TMAX", 
                        "WT02", "WT03", "WT04", "WT05", "WT07", "WT10", "WT11", "WT16", "WT17", "WT18")
  sdf_weather_data <- sdf_weather_data %>% 
    filter(is.null(QFlag) && Element %in% weather_elements) %>% 
    select(Station, Date, Element, Value)
  return(sdf_weather_data)
}