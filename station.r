# Reads the original txt file for stations
read_stations_org = function(
  path) 
{
  df_stations <- read.fwf(
    path, 
    widths = c(11,9,10,7,3,31,4,4,6), 
    header = FALSE,
    comment.char = '',
    strip.white = TRUE,
    col.names = list(
      "Id",
      "Lat",
      "Long",
      "Elevation",
      "State",
      "Name",
      "GSN",
      "HCN_CRN",
      "WMO_Id"))
  sdf_stations <- copy_to(
    sc, 
    df_stations, 
    name = 'stations_org', 
    overwrite = TRUE)
  sdf_stations <- sdf_stations %>% 
    select(Id, Lat, Long)
  return(sdf_stations)
}

# Generates stations table with added tile information
# (tile id as well as latitude and longitude of tile center)
generate_tiled_stations_table = function (
  path_source, 
  tile_size) 
{
  print("Generating tiled stations table ...")
  sdf_stations <- read_stations_org( path_source)
  sdf_tiled_stations <- sdf_stations %>% 
    # add tile id
    mutate( 
      Tile_Id = paste0(
        as.integer( Lat %/% tile_size + 90/tile_size),
        "-",
        as.integer( Long %/% tile_size + 180/tile_size))) %>%
    # add latitude and longitude of tile center
    mutate(
      Lat = 
        as.integer((Lat + 90) %/% tile_size) * 
        tile_size - 90 + (tile_size/2),
      Long = 
        as.integer((Long + 180) %/% tile_size) * 
        tile_size - 180 + (tile_size/2))
  print("... Finished generating tiled stations table.")
  return(sdf_tiled_stations)
}