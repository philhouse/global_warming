read_co2_global = function(path) {
  sdf_co2_global <- spark_read_csv(sc, "co2_global", 
                                   path, 
                                   header = TRUE, 
                                   infer_schema = TRUE
  )
  sdf_co2_global <- sdf_co2_global %>% filter(LENGTH(Year) == 4) # discards lines of the csv file that are no data records
  sdf_co2_global <- rename(sdf_co2_global, Total_in_mega_tons = 
                             Total_carbon_emissions_from_fossil_fuel_consumption_and_cement_production_million_metric_tons_of_C)
  sdf_co2_global <- sdf_co2_global %>% select(Year, Total_in_mega_tons)
  # Emission estimates are saved in million metric tons of carbon. To
  # convert these estimates to units of carbon dioxide (CO2), simply multiply
  # these estimates by 3.667.
  sdf_co2_global <- sdf_co2_global %>% mutate(Total_in_mega_tons = as.numeric(Total_in_mega_tons) * 3.667)
  sdf_co2_global <- sdf_co2_global %>% mutate(Year = as.integer(Year))
}