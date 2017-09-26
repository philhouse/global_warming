source("config.r", encoding = "UTF-8")

init = function (
  install = FALSE) 
{
  if (install == TRUE) install_packages()
  load_packages()
  start_spark()
}


install_packages = function () 
{
  install.packages("dplyr")
  install.packages("sparklyr")
  install.packages("shiny")
  
  install.packages("leaflet")
  install.packages("tidyr")
  
  install.packages("jsonlite")
  install.packages("geojsonio")
}

load_packages = function () 
{
  library(dplyr)
  library(sparklyr)
  library(ggplot2)
  library(shiny)
  library(tidyr)
  library(leaflet)
  library(jsonlite)
  library(geojsonio)
}

start_spark = function () 
{
  config <- spark_config()
  config$`sparklyr.shell.driver-memory` <- "6G"
  config$`sparklyr.shell.executor-memory` <- "6G"
  config$`spark.yarn.executor.memoryOverhead` <- "2G"
  sc <<- spark_connect(master = "local", version = "2.1.0", config = config)
}