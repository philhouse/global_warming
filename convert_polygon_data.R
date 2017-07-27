library(tibble)
source("init.r")
# Read Data, must be DataFrame for matchin PolygonID with data
data <- spark_read_csv(sc,"weather_data",path_weather_data) %>% filter(Year > 2013) %>% collect() %>% as.data.frame()
# Generate Polygon data from point data in our file for Polygons on Map just for our data
for(i in 1:nrow(data)){
  lng <- as.numeric(data[i,12]-5)
  lat <- as.numeric(data[i,11]-5)
  sub_data <- data[i,-(1:9)]
  # Create Polygons with data of point 
  polygon_data <- Polygons(list(Polygon(cbind(c(lng,lng,(lng+10),(lng+10)),c(lat,(lat+10),(lat+10),lat)))),i)
  # Append Data in properties to SpatialPolygonDataFrame
  sp_poly <- SpatialPolygonsDataFrame(SpatialPolygons(list(polygon_data)),data = sub_data)
  # First iteration: write new poly_data, following iterations: add data
  if(i > 1){
    poly_data <- poly_data + geojson_json(sp_poly)
  }else {
    poly_data <- geojson_json(sp_poly)
  }
  print(i)
  # In the laste iteration write the polygon data into a .geojson file
  if(i == nrow(data)){
    geojson_write(poly_data,file="shinyGlobe/geojson_polygons_data.geojson",geometry = "polygon",overwrite = TRUE)
  }
}
