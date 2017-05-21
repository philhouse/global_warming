# Entsprechenden Path auskommentieren
#path = "E:\\Big Data Prak\\ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\by_year\\1800.csv"
#path_stations = "E:\\Big Data Prak\\ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\ghcnd-stations___tmp.csv"
#path = "F:/Projekte/big_data_praktikum/by_year/2016.csv"
#path = "D:/Entwicklung/big-data-praktikum/data/1800.csv"

install.packages("xtable")
devtools::install_github("hadley/dplyr")
devtools::install_github("rstudio/sparklyr")
install.packages("ggplot2")
install.packages("RgoogleMaps")
install.packages("OpenStreetMap")
install.packages("ggmap")

library(sparklyr)
library(dplyr)
library(ggplot2)
library(RgoogleMaps)
library(OpenStreetMap)
library(ggmap)

# Spark Konfiguration mehr Arbeitsspeicher zur Verfuegung stellen
config <- spark_config()
config$`sparklyr.shell.driver-memory` <- "4G"
config$`sparklyr.shell.executor-memory` <- "4G"
config$`spark.yarn.executor.memoryOverhead` <- "1g"
sc <- spark_connect(master = "local", version = "2.1.0", config = config)

# Funktion für SQL-Queries
sqlfunction <- function(sc, block) {
  spark_session(sc) %>% invoke("sql", block)
}

data <- spark_read_csv(sc, "test", 
                       path, 
                       header=FALSE, 
                       infer_schema = FALSE,
                       columns = list(
                         V1 = "character",
                         V2 = "character",
                         V3 = "character",
                         V4 = "integer",
                         V5 = "character",
                         V6 = "character",
                         V7 = "character",
                         V8 = "character"
                         )
                       )
# Splittet CountryCode und StationCode fuer schnelleres Filtering
sqlfunction(sc,"SELECT SUBSTR(V1,1,2) AS country,SUBSTR(V1,3,9) AS station,V2,V3,V4,V5,V6,V7,V8 FROM test") %>% invoke("createOrReplaceTempView", "test")
data <- tbl(sc, "test")

# Plotbeispiel f?r 1800er CSV
dataf <- data %>% filter(V3 == "TMAX") %>% select(station, V2, V4) %>% filter(station == "E00100082")
d <- collect(dataf)
d
library(ggplot2)
ggplot(d, aes(as.Date(d$V2, "%Y%m%d"), d$V4)) + geom_point(aes(colour = d$station))

# Plotbeispiel GM Durchschnittstemp
dataf <- data %>% filter(country == "GM") %>% filter(V3 == "TMAX") %>% select(V2, V4) %>% group_by(V2) %>% summarise(temp = mean(V4/10))
d <- collect(dataf)
d %>% arrange(V2)
library(ggplot2)
ggplot(d, aes(as.Date(V2, "%Y%m%d"), temp)) + geom_point() + geom_smooth()

# Iteration Beispiel
data_count = vector("double", 6)
for(i in (1800:1805)) {
  data <- spark_read_csv(sc, "test", 
                         path = paste("F:/Projekte/big_data_praktikum/by_year/",i,".csv", sep = ""), 
                         header=FALSE, 
                         infer_schema = FALSE,
                         columns = list(
                           V1 = "character",
                           V2 = "character",
                           V3 = "character",
                           V4 = "integer",
                           V5 = "character",
                           V6 = "character",
                           V7 = "character",
                           V8 = "character"
                         )
  )
  # Splittet CountryCode und StationCode fuer schnellere Filter
  sqlfunction(sc,"SELECT SUBSTR(V1,1,2) AS country,SUBSTR(V1,3,9) AS station,V2,V3,V4,V5,V6,V7,V8 FROM test") %>% invoke("createOrReplaceTempView", "test")
  data <- tbl(sc, "test")
  
  dataf <- data %>% filter(country == "IT") %>% filter(V3 == "TMAX") %>% select(country, V4) %>% group_by(country) %>% summarise(temp = mean(V4/10)) %>% collect
  data_count[i] = as.double(dataf[1,2])
  print(data_count[i])
  #print(as.double(dataf[1,2]))
}

# Anzahl Messungen pro Land
data %>% filter(country == "GM") %>% summarise(count = n())

# Anzahl Messstationen pro Land
data %>% filter(country == "GM") %>% arrange(station) %>% group_by(station)  %>% summarise(count = n()) %>% summarise(count = n())

# Messarten
data %>% filter(country == "GM") %>% group_by(V3) %>% summarise(count = n())



########################################
# Pit

# 2016er CSV Operationen 
# DataFrame um Ländercode erweitern, weil substr() zu teuer ist
# ggf. nur bestimmte Spalten laden


# Auf allen Jahren operieren



########################################
# Phil

# Weltkarte mit Stationen

data_stations <- spark_read_csv(sc, "stations", 
                       path_stations, 
                       header=FALSE, 
                       infer_schema = FALSE, 
                       columns = list(
                         V1 = "character",
                         V2 = "double",
                         V3 = "double"
                       )
)
data_stations %>% arrange(V2) #filter(V2 >= 85.0)

###### RWorldMap #####
install.packages("rworldmap")
install.packages("rworldxtra")
library(rworldmap)
library(rworldxtra)
newmap <- getMap(resolution = "high")
#par=(mar=rep(0,4))
plot(newmap)
df_coordinates <- data_stations %>% select(V2,V3)
coordinates <- collect(df_coordinates)
points(coordinates$V3, coordinates$V2, col="red", cex=0.8) # pch=21 für ausgefüllte Punkte

###### ggmap #######
library(ggmap)

long = c(0, 45, 90, -90)
lat = c(0, 45, 90, -90)
who = c("Colmeal", "Portela", "Cabeça Ruiva", "Ilha do Lombo")
data = data.frame (long, lat, who)

map <- ggmap( get_map(location=c(0,0),zoom=1, maptype='satellite', scale = 2), size = c(600, 600), extent = 'normal')
map <- ggmap( get_googlemap(center=c(0,0), zoom=1, maptype='terrain', scale = 2), size = c(600, 600), extent = 'normal', darken = 0)
map <- ggmap( get_stamenmap(get_stamenmap(bbox = c(left = -95.80204, bottom = 29.38048, right = -94.92313, top = 30.14344), zoom = 10, maptype = "terrain")))

map + geom_point (
  data = data,
  aes (
    x = long, 
    y = lat, 
    fill = factor (who)
  ), 
  pch = 21, 
  colour = "white", 
  size = 6
)

###### RgoogleMaps #######
j <- c(90,-180)
m <- c(-90,180)
#If GetMap returns "HTTP status was '403 Forbidden'" in RStudio, go to "Tools"->"Global Options"->"Packages" and change the CRAN mirror to your country.
terrmap <- GetMap(center=c(0,0), zoom=1, maptype="terrain", SCALE=2, destfile="terrain.png")
terrmap <- GetMap.bbox(c(-90,90), c(-90, 90), SCALE=2, maptype="terrain", destfile="terrain.png")
PlotOnStaticMap(terrmap)
lat=seq(-90,90, 10)
lon=rep(0, length(lat))
PlotOnStaticMap(terrmap, lat, lon, destfile="terrain.png")

###### OpenStreetMap ########
j <- c(90,-180)
m <- c(-90,180)
map <- openmap(j,m,4,type="bing")
plot(map)#,removeMargin=FALSE)
getMapInfo()
launchMapHelper()

## Not run:
install.packages("maps")
library(maps)
#plot bing map in native mercator coords
map <- openmap(c(70,-179),
               c(-70,179),zoom=1,type='bing')
plot(map)
#using longlat projection lets us combine with the maps library
map_longlat <- openproj(map)
plot(map_longlat)
map("world",col="red",add=TRUE)
#robinson projection. good for whole globe viewing.
map_robinson <- openproj(map_longlat, projection=
                           "+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs")
plot(map_robinson)

# Weltkarte mit Anz. Stationen pro Land (als verschieden gro?e Punkte)
# Für Weltkarte mit Länderdaten: https://journal.r-project.org/archive/2011-1/RJournal_2011-1_South.pdf
#   - Weltkarte mit Punkten pro Land mapBubbles()


########################################
# Fragen:

# Weltkarte mit Score pro Land, um zu sehen, welche L?nder den Zeitraum am besten abdecken
# Tages_Score = Anz. t?gl. Messungen / Anz. m?glicher t?gl. Messungen (Anz. Stationen)
# Jahres_Score = Summe aller Tages_scores / 365 bzw. 366
# Score = Summe aller Jahres_Scores / Anz. Jahre



# Lineare Regression ?ber die z. B. TAVG-Werte in GM aller Jahre (Geradenanstieg entspricht j?hrlichem Temperaturanstieg) 
# besser
# Temperatur Plot ?ber alle Jahr
# F?r jedes Jahr den Durchschnittswert bilden von TMIN, TMAX, TAVG und diese drei "Kurven" (Punkte) ?ber alle Jahre plotten



# Plot der Eisschichtwerte in Antarktis (Keine FRTH-Werte in 2016, nur THIC in USA (Eisdicke auf Wasser))
# -


# Weltkarte mit Anstieg der Unwetter weltweit


# Weltkarte mit Anstieg des Niederschlags (PRCP) weltweit (Erderw?rmung ==> Mehr Kondensation)

