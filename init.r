source("config.r", encoding = "UTF-8")

tile_size <- 10

init = function (install = FALSE) {
  if (install == TRUE) install_packages()
  load_packages()
  start_spark()
}


install_packages = function () {
  install.packages("xtable")
  # devtools::install_github("hadley/dplyr")
  # devtools::install_github("rstudio/sparklyr")
  install.packages("dplyr")
  install.packages("sparklyr")
  install.packages("ggplot2")
  install.packages("rworldmap")
  install.packages("rworldxtra")
  install.packages("shiny")
  install.packages("leaflet")
  install.packages("jsonlite")
  install.packages("geojsonio")
}

load_packages = function () {
  library(sparklyr)
  library(dplyr)
  library(ggplot2)
  library(rworldmap)
  library(rworldxtra)
  library(shiny)
  library(tidyr)
  library(leaflet)
  library(jsonlite)
  library(geojsonio)
}

start_spark = function () {
  config <- spark_config()
  config$`sparklyr.shell.driver-memory` <- "4G"
  config$`sparklyr.shell.executor-memory` <- "4G"
  config$`spark.yarn.executor.memoryOverhead` <- "1g"
  sc <<- spark_connect(master = "local", version = "2.1.0", config = config)
}