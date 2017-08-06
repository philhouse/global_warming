source("init.r")
source("station.r")

#intall_packages()
init()
# ToDo: Generation not necessary for web app because we deliver the files. Just necessary for own/new data generation. Split into two setup files?
#delete: generate_stations_file(path_source=path_stations_org, path_target=path_stations, tile_size=10)
#better: generate_weather_data( path_source_weather = path_weather_yearly_org, path_source_stations = path_stations_org, path_target = path_weather_data, path_tmp = path_tmp)
