source("tile.r")

read_stations = function(path) {
  sdf_stations <- spark_read_csv(sc, "stations",
                 path = path,
                 header = TRUE,
                 infer_schema = TRUE
                 )
  sdf_stations %>% select(Id, Tile_Id)
}

read_stations_org = function(path) {
  df_stations <- read.fwf(path, 
                          widths = c(11,9,10,7,3,31,4,4,6), 
                          header = FALSE,
                          comment.char = '',
                          strip.white = TRUE,
                          col.names = list("Id",
                                           "Lat",
                                           "Long",
                                           "Elevation",
                                           "State",
                                           "Name",
                                           "GSN",
                                           "HCN_CRN",
                                           "WMO_Id")
  )
  sdf_stations <- copy_to(sc, df_stations, name = 'stations_org', overwrite = TRUE)
  sdf_stations <- sdf_stations %>% select(Id, Lat, Long)
  return(sdf_stations)
}

generate_stations_file = function (path_source, path_target, tile_size) {
  sdf_stations <- import_stations_org(path_source)
  
  #ToDo: Find another way to outsource the til_id calculation. 
  #      "It isn't possible to use custom functions in dplyr on a sql database."
  #      (https://stackoverflow.com/questions/37933109/using-a-custom-function-inside-of-dplyr-mutate)
  #      Try: vectorizing the tile function, e.g.: substr2 <- Vectorize(substr)
  #sdf_stations <- sdf_stations %>% mutate(Tile_Id = get_tile_id(tile_size, Lat, Long))
  sdf_stations <- sdf_stations %>% mutate(Tile_Id = paste(
    as.integer(Lat %/% tile_size + 90/tile_size), 
    "-", 
    as.integer(Long %/% tile_size + 180/tile_size), 
    sep=""))
  sdf_stations <- sdf_stations %>% select(Id, Tile_Id)
  write.table(sdf_stations, path_target, append=FALSE, na='', quote=FALSE, sep=",", col.names=TRUE, row.names=FALSE)
}