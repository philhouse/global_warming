
# Weather data

weather_elements <- c("PRCP", "SNOW", 
                     "TMAX", 
                     "WT02", "WT03", "WT04", "WT05", "WT07", "WT10", "WT11", "WT16", "WT17", "WT18")
year_start <- 1763
for(i in (year_start:2017)) {
  sdf_weather_data <- spark_read_csv(sc, "weather_data", 
                         path = paste(path_weather_yearly,i,".csv", sep = ""), 
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
  sdf_weather_data <- sdf_weather_data %>% 
    filter(is.null(QFlag) && Element %in% weather_elements) %>% 
    select(Station, Date, Element, Value)
  # ...
}


# stations

df_stations <- read.fwf(path_stations, 
                          widths = c(11,9,10,7,3,31,4,4,6), 
                          header = FALSE,
                          comment.char = '',
                          strip.white = TRUE,
                          col.names = list("Id",
                                           "Lat",
                                           "Long",
                                           "Elevation",
                                           "State",
                                           "Name",
                                           "GSN",
                                           "HCN_CRN",
                                           "WMO_Id")
)
sdf_stations <- copy_to(sc, df_stations, name = 'stations', overwrite = TRUE)
sdf_stations <- sdf_stations %>% select(Id, Lat, Long)


# CO2 Emission global

sdf_co2_global <- spark_read_csv(sc, "co2_global", 
                                 path_co2_global, 
                                 header = TRUE, 
                                 infer_schema = TRUE
)
sdf_co2_global <- sdf_co2_global %>% filter(LENGTH(Year) == 4) # discards lines of the csv file that are no data records
sdf_co2_global <- rename(sdf_co2_global, Total_in_mega_tons = 
                           Total_carbon_emissions_from_fossil_fuel_consumption_and_cement_production_million_metric_tons_of_C)
sdf_co2_global <- sdf_co2_global %>% select(Year, Total_in_mega_tons)
sdf_co2_global <- sdf_co2_global %>% mutate(Total_in_mega_tons = as.numeric(Total_in_mega_tons))
sdf_co2_global <- sdf_co2_global %>% mutate(Year = as.integer(Year))


# CO2 Emission per nation

sdf_co2_nation <- spark_read_csv(sc, "co2_nation", 
                                 path_co2_nation, 
                                 header = TRUE, 
                                 infer_schema = TRUE,
                                 null_value = '.'
)
sdf_co2_nation <- sdf_co2_nation %>% filter(LENGTH(Year) == 4) # discards lines of the csv file that are no data records
sdf_co2_nation <- rename(sdf_co2_nation, Total_in_kilo_tons = 
                           Total_CO2_emissions_from_fossilfuels_and_cement_production_thousand_metric_tons_of_C)
sdf_co2_nation <- sdf_co2_nation %>% select(Nation, Year, Total_in_kilo_tons)
sdf_co2_nation <- sdf_co2_nation %>% mutate(Total_in_kilo_tons = as.numeric(Total_in_kilo_tons))
sdf_co2_nation <- sdf_co2_nation %>% mutate(Year = as.integer(Year))

