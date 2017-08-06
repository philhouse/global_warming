# set your paths here

# sources of original data
perfix_data_org <- "R:\\Big Data Prak\\data_org\\"
path_weather_yearly_org <- paste0(perfix_data_org, "ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\by_year\\")
path_stations_org <- paste0(perfix_data_org, "ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\ghcnd-stations.txt")
path_co2_global <- paste0(perfix_data_org, "cdiac.ornl.gov\\ftp\\ndp030\\CSV-FILES\\global.1751_2014.csv")

# sources of generated data
path_processed <- "R:\\Big Data Prak\\data_processed\\"
path_weather_data <- paste0(path_processed, "weather_data\\")

# folder for temporary data, that is created while generating the weather_data for the shiny web application
path_tmp <- "R:\\Big Data Prak\\tmp"
#path_stations <- paste0(path_processed, "stations.txt")
#path_baselines <- "R:\\Big Data Prak\\baselines\\weather_per_tile"
#path_baseline <- paste0(path_processed, "baseline\\*")
#path_tiles_per_year = "R:\\Big Data Prak\\data_analyses\\tiles_per_year.csv"
#path_tiles_initial <- paste0(path_processed, "tiles_initial.csv")