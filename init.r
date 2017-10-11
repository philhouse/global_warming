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
  # Parameters for Spark
  spark_version <- "2.1.0"
  my_spark_config <- spark_config()
  my_spark_config$`sparklyr.shell.driver-memory` <- "6G"
  my_spark_config$`sparklyr.shell.executor-memory` <- "6G"
  my_spark_config$`spark.yarn.executor.memoryOverhead` <- "2G"
  my_spark_config$`spark.network.timeout` <- "600s"
  my_spark_config$`spark.driver.extraJavaOptions` <- "-XX:+UseG1GC"
  my_spark_config$`spark.executor.extraJavaOptions` <- "-XX:+UseG1GC"
  sc <<- spark_connect(master = "local", version = spark_version, config = my_spark_config)
}