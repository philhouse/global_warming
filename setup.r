# Installs libraries, 
# generates new data (optional) and 
# runs the web application

# install needed libraries
source("init.r")
install_packages()

# optional (since the files are included already): 
# generate a new weather data table
source("init.r")
source("generate_weather_data.r")
generate_weather_data(
  path_weather_yearly_org,
  path_weather_data,
  path_stations_org,
  tile_size,
  path_tmp,
  year_start_baseline,
  year_span_baseline,
  year_start_data,
  year_end_data,
  baseline_measurement_coverage_threshold
)
# and convert it to polygon data for the web application 
source("convert_polygon_data.R")

# start the web application
source("main.R")