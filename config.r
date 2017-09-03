# set your paths here

# sources of original data (Weather meassurements of one year can not be spread acros multiple files!)
perfix_data_org <- "R:\\Big Data Prak\\data_org\\"
path_weather_yearly_org <- paste0(perfix_data_org, "ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\by_year\\")
path_stations_org <- paste0(perfix_data_org, "ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\ghcnd-stations.txt")
path_co2_global <- paste0(perfix_data_org, "cdiac.ornl.gov\\ftp\\ndp030\\CSV-FILES\\global.1751_2014.csv")

# sources of generated data
path_processed <- "R:\\Big Data Prak\\data_processed\\"
path_weather_data <- paste0(path_processed, "weather_data\\")

# folder for temporary data, that is created while generating the weather_data for the shiny web application
path_tmp <- "R:\\Big Data Prak\\tmp\\"
#path_stations <- paste0(path_tmp, "stations")
#path_baselines <- paste0(path_tmp, "baselines") #\\weather_per_tile"
#path_baseline <- paste0(path_processed, "baseline\\*")
#path_tiles_per_year = paste0(path_tmp, "tiles_per_year") #"R:\\Big Data Prak\\data_analyses\\tiles_per_year.csv"
#path_tiles_initial <- paste0(path_processed, "tiles_initial.csv")


# set your parameters here
year_start_data <- 1917
year_end_data <- 2016
year_start_baseline <- year_start_data
year_span_baseline <- 30
baseline_measurement_coverage_threshold <- 0.8 # used to reject baseline tiles by coverage of days and years regarding measurement records
tile_size <- 10 # set a natural even number