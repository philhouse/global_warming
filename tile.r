get_tile_id = function(tile_size, lat, long) {
  lat_id = get_tile_lat_id(tile_size, lat)
  long_id = get_tile_long_id(tile_size, long)
  
  tile_id = paste(lat_id, "-", long_id, sep="")
}

get_tile_lat_id = function(tile_size, lat) {
  lat_id = as.integer(lat %/% tile_size + 90/tile_size)
}

get_tile_long_id = function(tile_size, long) {
  long_id = as.integer(long %/% tile_size + 180/tile_size)
}

get_tile_center = function(tile_id, tile_size) {
  lat_long <- strsplit(tile_id, "-")[[1]]
  lat <- (as.numeric(lat_long[1]) - 90/tile_size) * tile_size
  long <- (as.numeric(lat_long[2]) - 180/tile_size) * tile_size
  center <- c( lat + (tile_size/2), long + (tile_size/2))
}
