# ToDo: Auslagern in config file
path_weather_yearly <- "R:\\Big Data Prak\\data_org\\ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\by_year\\"
path_stations_yearly <- "R:\\Big Data Prak\\stations\\"
path_stations_org <- "R:\\Big Data Prak\\data_org\\ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\ghcnd-stations.txt"
path_stations <- "R:\\Big Data Prak\\data_processed\\stations.txt"
path_co2_global <- "R:\\Big Data Prak\\data_org\\cdiac.ornl.gov\\ftp\\ndp030\\CSV-FILES\\global.1751_2014.csv"
path_co2_nation <- "R:\\Big Data Prak\\data_org\\cdiac.ornl.gov\\ftp\\ndp030\\CSV-FILES\\nation.1751_2014.csv"
path_processed <- "R:\\Big Data Prak\\data_processed\\"
path_baseline <- paste(path_processed, "baseline\\*", sep = "")
path_tiles_initial = "R:\\Big Data Prak\\data_processed\\tiles_initial.csv"
path_weather_data = paste(path_processed, "weather_data\\", sep="")

tile_size <- 10

init = function (install = FALSE) {
  if (install == TRUE) install_packages()
  load_packages()
  start_spark()
}


install_packages = function () {
  install.packages("xtable")
  devtools::install_github("hadley/dplyr")
  devtools::install_github("rstudio/sparklyr")
  install.packages("ggplot2")
  install.packages("rworldmap")
  install.packages("rworldxtra")
  install.packages("shiny")
}

load_packages = function () {
  library(sparklyr)
  library(dplyr)
  library(ggplot2)
  library(rworldmap)
  library(rworldxtra)
  library(shiny)
  library(tidyr)
}

start_spark = function () {
  config <- spark_config()
  config$`sparklyr.shell.driver-memory` <- "4G"
  config$`sparklyr.shell.executor-memory` <- "4G"
  config$`spark.yarn.executor.memoryOverhead` <- "1g"
  sc <<- spark_connect(master = "local", version = "2.1.0", config = config)
}