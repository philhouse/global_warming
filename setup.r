source("init.r")
source("station.r")

#intall_packages()
init()
generate_stations_file(path_source=path_stations_org, path_target=path_stations, tile_size=10)
