# Use this file to configure source, target and temporary paths as well as different parameters


# Paths:

prefix_data <- "R:\\Big Data Prak\\" #"D:/Entwicklung/big-data-praktikum/"

# Path prefix for original data
prefix_data_org <- paste0(
  prefix_data, 
  "data_org\\")
# Path to the yearly CSV files of the GHCN Daily data set
# (Weather data is expected to be one csv file per year!)
path_weather_yearly_org <- paste0(
  prefix_data_org, 
  "ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\by_year\\")
# Path to the weather stations file taken from
# ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
path_stations_org <- paste0(
  prefix_data_org, 
  "ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\ghcnd-stations.txt")
# Path to the emission data CSV file taken from
# http://cdiac.ornl.gov/ftp/ndp030/CSV-FILES/nation.1751_2014.csv
path_co2_global <- paste0(
  prefix_data_org, 
  "cdiac.ornl.gov\\ftp\\ndp030\\CSV-FILES\\global.1751_2014.csv")

# Path for generated data
path_processed <- paste0(
  prefix_data, 
  "data_processed\\")
# Path for generated weather data
path_weather_data <- paste0(
  path_processed, 
  "weather_data\\")

# Path for temporary data, that is created while generating the data for the shiny web application
path_tmp <- paste0(
  prefix_data, 
  "tmp\\")



# Parameters for data generation:

# First year considered for weather data generation
year_start_data <- 1917
# Last year considered for weather data generation
year_end_data <- 2016
# First year considered for the weather baseline generation
year_start_baseline <- year_start_data
# Number of years to take into account for the weather baseline generation
year_span_baseline <- 30
# Defines during the baseline generation which tiles should be used,
# because they have enough measurements to cover at least 
# (baseline_measurement_coverage_threshold * 100) % of the baseline span.
baseline_measurement_coverage_threshold <- 0.8 # Decimal number of range [0..1]
# Degrees of longitude/latitude defining the size of
# each tile (quadrat) the world map is partitioned into
tile_size <- 10 # set a natural even number