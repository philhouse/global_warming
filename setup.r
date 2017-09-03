# installs libraries, generates data files (optional) and executes the web application

# install needed libraries
source("init.r")
intall_packages()

# optional: generate new weather data files for the web application (optional since the files are included already)
source("generate_weather_data.r")

# start the web application
source("main.R")