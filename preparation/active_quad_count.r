# Erstellt Plot mit Anzahl der Kacheln die abgedeckt werden über die Jahre, um damit das optimale Startjahr bzgl. Baselines bestimmen zukönnen (möglichst frühes Jahr mit großer Abdeckung)

source("../station.r")

# Plot zeigt: 1957 ist ein gutes Startjahr

filename_prefix_stations_year = "stations_"

df_stations <- read.stations_org(path_stations)

# Exportiere Liste aller Stationen in eine CSV pro Jahr. Spalten: Station, Lat, Long sowie jeweils Lat_Id, Long_Id und Id für die Kachelgrößen 2.5, 5 und 10
year_start <- 1763
for(i in (year_start:2017)) {
  sdf_stations_per_year <- read_weather_data(path_weather_yearly_org, i)
  
  # Erstelle Liste der Stationen, die im jeweiligen Jahr aktiv waren.
  sdf_stations_per_year <- sdf_stations_per_year %>% group_by(Station)  %>% summarise()
  # Join mit Stationsdatei wegen Koordinaten
  sdf_stations_per_year <- inner_join(sdf_stations_per_year, sdf_stations, by=c("Station" = "Id"))
  # Mapping Koordinaten zu 5er und 10er Kacheln (Id)
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Year = as.integer(i))
  tile_size = 2.5
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Tile_2_5_Lat_Id = as.integer(Lat %/% tile_size + 90/tile_size)) # Normalisierung auf Ids >=0
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Tile_2_5_Long_Id = as.integer(Long %/% tile_size + 180/tile_size))
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Tile_2_5_Id = paste(
    {Lat %/% tile_size + 90/tile_size}, 
    "-", 
    {Long %/% tile_size + 180/tile_size}, sep=""))
  tile_size = 5
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Tile_5_Lat_Id = as.integer(Lat %/% tile_size + 90/tile_size))
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Tile_5_Long_Id = as.integer(Long %/% tile_size + 180/tile_size))
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Tile_5_Id = paste(Tile_5_Lat_Id, "-", Tile_5_Long_Id, sep=""))
  tile_size = 10
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Tile_10_Lat_Id = as.integer(Lat %/% tile_size + 90/tile_size))
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Tile_10_Long_Id = as.integer(Long %/% tile_size + 180/tile_size))
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Tile_10_Id = paste(Tile_10_Lat_Id, "-", Tile_10_Long_Id, sep=""))
  write.table(sdf_stations_per_year, paste(path_stations_year, "stations_years.csv", sep=""), append=TRUE, na='', quote=TRUE, sep=",", col.names={i == year_start}, row.names=FALSE)
  print(i)
}


# Lese Jahresliste (single file) der Stations und z?hle die (aktiven) Kacheln pro Jahr

sdf_stations_per_year <- spark_read_csv(sc, "stations_year", 
                                        path = paste(path_stations_year, "stations_years.csv", sep = ""), 
                                        header = TRUE, 
                                        infer_schema = TRUE
)
# count tiles
tile_2_5_counts <- sdf_stations_per_year %>% group_by(.dots=c(Year,Quad_2_5_Id)) %>% summarise() %>% nrow()
tile_5_counts <- sdf_stations_per_year %>% group_by(.dots=c(Year,Quad_5_Id)) %>% summarise() %>% nrow()
tile_10_counts <- sdf_stations_per_year %>% group_by(.dots=c(Year,Quad_10_Id)) %>% summarise() %>% nrow()
df_tmp <- data.frame(c(tile_2_5_counts), c(tile_5_counts), c(tile_10_counts))
df_tiles <- rbind(df_tmp, df_tiles)
print(i)


# Lese Jahreslisten der Stations und z?hle die (aktiven) Kacheln pro Jahr

newmap <- getMap(resolution = "high")
df_tiles <- data.frame()
year_start <- 1763
for(i in (year_start:2016)) {
  sdf_stations_per_year <- spark_read_csv(sc, "stations_year", 
                                          path = paste(path_stations_year, filename_prefix_stations_year, i, ".csv", sep = ""), 
                                          header = TRUE, 
                                          infer_schema = TRUE
  )
  
  # Export WorldMap Plot per year
  png(paste(path_stations_year, "stations_", i, ".png", seq=""), width = 2560, height = 1377)
  plot(newmap)
  coordinates <- sdf_stations_per_year %>% select(Lat,Long) %>% collect()
  points(coordinates$Long, coordinates$Lat, col="red", cex=0.8)
  #ggsave(paste(path_stations_year, "plot_", 1900, ".png", seq=""), device = "png")
  dev.off()
  
  # count tiles
  tile_2_5_count <- sdf_stations_per_year %>% group_by(.dots=c(Year,Quad_2_5_Id)) %>% summarise() %>% nrow()
  tile_5_count <- sdf_stations_per_year %>% group_by(Quad_5_Id) %>% summarise() %>% nrow()
  tile_10_count <- sdf_stations_per_year %>% group_by(Quad_10_Id) %>% summarise() %>% nrow()
  df_tmp <- data.frame(c(i), c(tile_2_5_count), c(tile_5_count), c(tile_10_count))
  df_tiles <- rbind(df_tmp, df_tiles)
  print(i)
}
names(df_tiles) <- c("Year", "Tile_2_5_count", "Tile_5_count", "Tile_10_count")
ggplot(df_tiles, aes(x = as.integer(Year))) + 
  geom_point(aes(y = df_tiles$Tile_2_5_count, colour = "2.5 sized tiles")) +
  geom_point(aes(y = df_tiles$Tile_5_count, colour = "5 sized tiles")) +
  geom_point(aes(y = df_tiles$Tile_10_count, colour = "10 sized tiles"))
