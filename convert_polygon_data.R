source("init.r")
# Read Data, must be DataFrame for matchin PolygonID with data
data <- spark_read_csv(sc,"weather_data",path_weather_data) %>% arrange(Year) %>% collect() %>% as.data.frame()
past_element_year <- 1
start_row <- 1
n <- nrow(data)
# Generate Polygon data from point data in our file for Polygons on Map just for our data
edge_distance = tile_size/2
for(i in start_row:n){
  lng <- as.numeric( data[i,11] - edge_distance) #ToDo: Get data by column name
  lat <- as.numeric( data[i,10] - edge_distance)
  sub_data <- data[i,-(1:9)]
  # Create Polygons with data of point 
  polygon_data <- Polygons( list( Polygon( cbind( c( lng, lng, (lng + tile_size), (lng + tile_size)), 
                                                  c( lat, (lat + tile_size), (lat + tile_size), lat)))),
                            i)
  # Append Data in properties to SpatialPolygonDataFrame
  sp_poly <- SpatialPolygonsDataFrame(SpatialPolygons(list(polygon_data)),data = sub_data)
  
  # First iteration and at the end of each year set: write yearly set into geojson file and create new poly_data, following iterations: add data to previous poly_data
  if((i == 1) || !(sub_data$Year == past_element_year)){
    if(!(i==1)){
      geojson_write(poly_data,file=paste(path_processed,"polygons_weather_data\\geojson_polygons_data_",past_element_year,".geojson",sep = ""),geometry = "polygon")
      print(i)
    }
    past_element_year <- sub_data$Year
    poly_data <- geojson_json(sp_poly)
  } else {
    poly_data <- poly_data + geojson_json(sp_poly)
  }
  # Last iteration the data has to be written
  if(i == nrow(data)){
    geojson_write(poly_data,file=paste(path_processed,"polygons_weather_data\\geojson_polygons_data_",past_element_year,".geojson",sep = ""),geometry = "polygon")
  }
}