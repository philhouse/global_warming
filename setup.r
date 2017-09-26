# installs libraries, 
# generates data files (optional) and 
# executes the web application

# install needed libraries
source("init.r")
install_packages()

# optional: 
# generate new weather data files for the web application 
# (optional since the files are included already)
log <- file("output.log", open="wt")
sink(log, type="message")
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

# start the web application
source("main.R")