# Entsprechenden Path auskommentieren
#path_year = "R:\\Big Data Prak\\ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\by_year\\"
#path_stations_year = "R:\\Big Data Prak\\stations\\"
#path_stations = "R:\\Big Data Prak\\ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\ghcnd-stations.txt"
#path_co2_global = "R:\\Big Data Prak\\cdiac.ornl.gov\\ftp\\ndp030\\CSV-FILES\\global.1751_2014.csv"
#path_co2_nation = "R:\\Big Data Prak\\cdiac.ornl.gov\\ftp\\ndp030\\CSV-FILES\\nation.1751_2014.csv"
#path_year = "F:/Projekte/big_data_praktikum/by_year/"
#path_year = "D:/Entwicklung/big-data-praktikum/data/"

install.packages("xtable")
devtools::install_github("hadley/dplyr")
devtools::install_github("rstudio/sparklyr")
install.packages("ggplot2")
install.packages("rworldmap")
install.packages("rworldxtra")
install.packages("shiny")

library(sparklyr)
library(dplyr)
library(ggplot2)
library(rworldmap)
library(rworldxtra)
library(shiny)

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
                       path = paste(path_year,"1800.csv", sep = ""), 
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
# Splittet CountryCode und StationCode fuer schnelleres Filtering, V6 (QFLAG) muss leer sein (keine Konsistenzfehler)
sqlfunction(sc,"SELECT SUBSTR(V1,1,2) AS country,SUBSTR(V1,3,9) AS station,V2,V3,V4,V5,V6,V7,V8 FROM test WHERE (V6 IS NULL)") %>% invoke("createOrReplaceTempView", "test")
data <- tbl(sc, "test")

# Plotbeispiel für 1800er CSV
dataf <- data %>% filter(V3 == "TMAX") %>% select(station, V2, V4) %>% filter(station == "E00100082")
d <- collect(dataf)
d
ggplot(d, aes(as.Date(d$V2, "%Y%m%d"), d$V4)) + geom_point(aes(colour = d$station))

# Plotbeispiel GM Durchschnittstemp
dataf <- data %>% filter(country == "GM") %>% filter(V3 == "TMAX") %>% select(V2, V4) %>% group_by(V2) %>% summarise(temp = mean(V4/10))
d <- collect(dataf)
d %>% arrange(V2)
ggplot(d, aes(as.Date(V2, "%Y%m%d"), temp)) + geom_point() + geom_smooth()

# Iteration Beispiel
data_count = vector("double", 21)
for(i in (1763:2017)) {
  data <- spark_read_csv(sc, "test", 
                         path = paste(path_year,i,".csv", sep = ""), 
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
  #sqlfunction(sc,"SELECT SUBSTR(V1,1,2) AS country,SUBSTR(V1,3,9) AS station,V2,V3,V4,V5,V6,V7,V8 FROM test WHERE (V6 IS NULL)") %>% invoke("createOrReplaceTempView", "test")
  #data <- tbl(sc, "test")
  
  data <- tbl(sc, "test")
  org <- data %>% summarise(count = n()) %>% collect
  droped <- data %>% filter(!is.null(V6)) %>% summarise(count = n()) %>% collect
  org = as.double(org[1,1])
  droped = as.double(droped[1,1])
  print(paste(i, org, droped, droped/org*100, sep=","))
  
  
  #dataf <- data %>% filter(country == "IT") %>% filter(V3 == "TMAX") %>% select(country, V4) %>% group_by(country) %>% summarise(temp = mean(V4/10)) %>% collect
  #data_count[i] = as.double(dataf[1,2])
  #print(data_count[i])
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

# Einlesen der CO2 Emissionswerte
sdf_co2_global <- spark_read_csv(sc, "co2_global", 
                                 path_co2_global, 
                                 header=TRUE, 
                                 infer_schema = TRUE
)
# Data Cleaning
sdf_co2_global <- sdf_co2_global %>% filter(LENGTH(Year) == 4)

sdf_co2_global <- rename(sdf_co2_global, Total = Total_carbon_emissions_from_fossil_fuel_consumption_and_cement_production_million_metric_tons_of_C)
#sdf_co2_global <- rename(sdf_co2_global, Gas_fuel = Carbon_emissions_from_gas_fuel_consumption)
#sdf_co2_global <- rename(sdf_co2_global, Liquid_fuel = Carbon_emissions_from_liquid_fuel_consumption)
#sdf_co2_global <- rename(sdf_co2_global, Solid_fuel = Carbon_emissions_from_solid_fuel_consumption)
#sdf_co2_global <- rename(sdf_co2_global, Cement_production = Carbon_emissions_from_cement_production)
#sdf_co2_global <- rename(sdf_co2_global, Gas_flaring = Carbon_emissions_from_gas_flaring)
#sdf_co2_global <- rename(sdf_co2_global, Per_capita = Per_capita_carbon_emissions_metric_tons_of_carbon_after_1949_only)

sdf_co2_global <- transform(sdf_co2_global, Total = as.numeric(Total))
sdf_co2_global <- transform(sdf_co2_global, Year = as.Date(Year, "%Y"))
df_co2_global <- sdf_co2_global %>% select(Year, Total) %>% collect
ggplot(df_co2_global, Year, Total) + geom_point() + geom_smooth()
ggplot(df_co2_global, aes(as.Date(Year, "%Y"), Total)) + geom_point() + geom_smooth()


# Weltkarte mit Stationen

data_stations <- read.fwf(path_stations, 
                          widths = c(11,9,10,7,3,31,4,4,6), 
                          header = FALSE,
                          comment.char='',
                          strip.white = TRUE)

###### RWorldMap #####
newmap <- getMap(resolution = "high")
#par=(mar=rep(0,4))
plot(newmap)
df_coordinates <- data_stations %>% select(V2,V3)
coordinates <- collect(df_coordinates)
points(coordinates$V3, coordinates$V2, col="red", cex=0.8) # pch=21 für ausgefüllte Punkte

# Weltkarte mit Anz. Stationen pro Land (als verschieden große Punkte)
# Für Weltkarte mit Länderdaten: https://journal.r-project.org/archive/2011-1/RJournal_2011-1_South.pdf
#   - Weltkarte mit Punkten pro Land mapBubbles()


########################################

## Datenabdeckung

# Stations Weltkarte

# Weltkarte mit Score pro Land, um zu sehen, welche Länder den Zeitraum am besten abdecken
# Tages_Score = Anz. tägl. Messungen / Anz. möglicher tägl. Messungen (Anz. Stationen)
# Jahres_Score = Summe aller Tages_scores / 365 bzw. 366
# Score = Summe aller Jahres_Scores / Anz. Jahre


## Fragen:

# Weltkarte mit Anstieg der Temperatur pro z.B. 10°x10° Lat/Long Fläche)
# Dropdown-Menü: Ganzes Jahr, Sommer, Winter
# Checkbox: Temperaturen, CO2 (ToDo: Kann Karte Länder in Coordinaten (für Plot) mappen?), Niederschlag (PRCP+SNOW), Unwetter (evtl. für jeden Typ eine Checkbox und dann wird aktiviertes summiert. Details über Verteilung erfährt man beim Anklicken des Objektes)
# - Weltkarte mit Jahres-Slider zeigt Temperaturdifferenzen zur Baseline
# (- Weltkarte mit Regressionsanstiegen der Werte über den gesamten Zeitraum; evtl. als Checkbox und dann wird Jahres-Slider deaktiviert)
# Darstellung:
#   Für Wertebereiche mit pos. und neg. werden Objekte eingefärbt, z.B. Temperatur min. minus nach max. plus als (rgb(0,0,255), ..., rgb(100,100,255), ..., rgb(255,255,255), ..., rgb(255,0,0),)
#   Mehrere Objekte mit pos/neg Bereich werden als unterschiedliche Objekte angezeigt (Kreis, Quadrat, etc.) oder man kann nicht beides gleichzeitig anzeigen
#   Für Anstiege: Details bei Mouse-Over: Welchen Zeitraum (Welche Jahre) deckt Regressionsgerade ab?

# globaler Plot
# Lineare Regression über die z. B. TAVG-Werte in GM aller Jahre (Geradenanstieg entspricht jährlichem Temperaturanstieg)
# besser
# Temperatur Plot über alle Jahr
# Für jedes Jahr den Durchschnittswert bilden von TMIN, TMAX, TAVG und diese drei "Kurven" (Punkte) über alle Jahre plotten
# CO2 Plot überlegt mit globaler Temp. pro Jahr
# Unwetter überlegt (auch einzeln klickbar evtl. einzelne Kurven)



# Weltkarte mit Anstieg des Niederschlags (PRCP) weltweit (Erderwärmung ==> Mehr Kondensation)

# Weltkarte mit Anstieg der Unwetter weltweit
# Plot mit Anstiegen der einzelnen Unwetterarten
# WT** = Weather Type where ** has one of the following values:
# 02 = Heavy fog or heaving freezing fog
# 03 = Thunder
# 04 = Ice pellets, sleet, snow pellets, or small hail 
# 05 = Hail (may include small hail)
# 07 = Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction
# 10 = Tornado, waterspout, or funnel cloud 
# 11 = High or damaging winds
# 16 = Rain (may include freezing rain, drizzle, and freezing drizzle) 
# 17 = Freezing rain 
# 18 = Snow, snow pellets, snow grains, or ice crystals
# (19 = Unknown source of precipitation )

#data_count <- NULL
sdf_weather_type <- NULL
year_start = 1763
for(i in (year_start:2017)) {
  data <- spark_read_csv(sc, "test", 
                         path = paste(path_year,i,".csv", sep = ""), 
                         header=FALSE, 
                         infer_schema = FALSE,
                         columns = list(
                           Station = "character",
                           Date = "character",
                           Element = "character",
                           Value = "integer",
                           MFlag = "character",
                           QFlag = "character",
                           SFlag = "character",
                           Time = "character"
                         )
  )
  sdf_filtered <- data %>% filter(Element %like% 'WT%' && is.null(QFlag)) %>% select(Station,Date,Element)
  count_new <- as.integer({sdf_filtered %>% summarise(count = n()) %>% collect}[1,1])
  if (i == year_start) {sdf_weather_type <- sdf_filtered}
  else sdf_weather_type <- rbind(sdf_weather_type, sdf_filtered)
  #dataf <- data %>% filter(V3 %like% 'WT%') %>% summarise(count = n()) %>% collect
  #dataf <- data %>% filter(V3 %like% 'WT%') %>% summarise(count = n()) %>% collect
  #data_count <- c(data_count, as.integer(dataf[1,1]))
  count_all <- as.integer({sdf_weather_type %>% summarise(count = n()) %>% collect}[1,1])
  print(paste(i, count_all, count_new))
}
write.csv(sdf_weather_type %>% collect, "E:\\Big Data Prak\\weather_type\\weather_type.csv", na='', quote = FALSE, row.names = FALSE, col.names = FALSE)

# Verteilung tmin, tmax, tavg

sdf_weather_type <- NULL
year_start = 1763
for(i in (year_start:2017)) {
  data <- spark_read_csv(sc, "test", 
                         path = paste(path_year,i,".csv", sep = ""), 
                         header=FALSE, 
                         infer_schema = FALSE,
                         columns = list(
                           Station = "character",
                           Date = "character",
                           Element = "character",
                           Value = "integer",
                           MFlag = "character",
                           QFlag = "character",
                           SFlag = "character",
                           Time = "character"
                         )
  )
  sdf_filtered <- data %>% filter(Element %like% 'WT%' && is.null(QFlag)) %>% select(Station,Date,Element)
  count_new <- as.integer({sdf_filtered %>% summarise(count = n()) %>% collect}[1,1])
  if (i == year_start) {sdf_weather_type <- sdf_filtered}
  else sdf_weather_type <- rbind(sdf_weather_type, sdf_filtered)
  #dataf <- data %>% filter(V3 %like% 'WT%') %>% summarise(count = n()) %>% collect
  #dataf <- data %>% filter(V3 %like% 'WT%') %>% summarise(count = n()) %>% collect
  #data_count <- c(data_count, as.integer(dataf[1,1]))
  count_all <- as.integer({sdf_weather_type %>% summarise(count = n()) %>% collect}[1,1])
  print(paste(i, count_all, count_new))
}
write.csv(sdf_weather_type %>% collect, "E:\\Big Data Prak\\weather_type\\weather_type.csv", na='', quote = FALSE, row.names = FALSE, col.names = FALSE)


# Plot der Eisschichtwerte in Antarktis (Keine FRTH-Werte in 2016, nur THIC in USA (Eisdicke auf Wasser))
# -


#Data Cleaning
data_cleaning <- spark_read_csv(sc, "test", 
                                path = "E:\\Big Data Prak\\Data_Cleaning.csv", 
                                header=FALSE, 
                                infer_schema = FALSE,
                                columns = list(
                                  V1 = "character",
                                  V2 = "integer",
                                  V3 = "integer",
                                  V4 = "numeric"
                                ))
data_cleaning_df <- collect(data_cleaning)
ggplot(data_cleaning_df, aes(as.Date(V1, "%Y"), V4)) + geom_point() + geom_smooth()



######################################################
########## Bestimmt Startjahr für Baselines ##########
######################################################
#### CSV mit Stationsanzahl über alle Jahre
year_start <- 1763
for(i in (year_start:2017)) {
  sdf_weather_data <- spark_read_csv(sc, "weather_data", 
                                     path = paste(path_year,i,".csv", sep = ""), 
                                     header = FALSE, 
                                     infer_schema = FALSE,
                                     columns = list(
                                       Station = "character",
                                       Date = "character",
                                       Element = "character",
                                       Value = "integer",
                                       MFlag = "character",
                                       QFlag = "character",
                                       SFlag = "character",
                                       Time = "character"
                                     )
  )
  counts <- sdf_weather_data %>% group_by(station)  %>% summarise() %>% collect() %>% nrow()
  counts
  write.table(data.frame(i, as.integer(counts[1])), "E:\\Big Data Prak\\station_counts.csv", append=TRUE, na='', quote = FALSE, sep=",", col.names=FALSE, row.names = FALSE)
}

#### Besser: Anzahl der Quadrate die abgedeckt werden über die Jahre.
# Matrix: Zeilen sind Quadrate, Spalten sind Jahre, Zellenwerte sind Anzahl der Stationen im jeweiligen Quadrat.
# Summiere dann die Anzahl aktiver Quadrate in einem Jahr ggf. mit Threshold zur Aktivierung

# Lese Stationsdatei
df_stations <- read.fwf(path_stations, 
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
sdf_stations <- copy_to(sc, df_stations, name = 'stations', overwrite = TRUE)
sdf_stations <- sdf_stations %>% select(Id, Lat, Long)

# Exportiere Liste aller Stationen in eine CSV pro Jahr. Spalten: Station, Lat, Long sowie jeweils Lat_Id, Long_Id und Id für die Quadratgrößen 2.5, 5 und 10
year_start <- 1763
for(i in (year_start:2017)) {
  sdf_stations_per_year <- spark_read_csv(sc, "weather_data", 
                                          path = paste(path_year,i,".csv", sep = ""), 
                                          header = FALSE, 
                                          infer_schema = FALSE,
                                          columns = list(
                                            Station = "character",
                                            Date = "character",
                                            Element = "character",
                                            Value = "integer",
                                            MFlag = "character",
                                            QFlag = "character",
                                            SFlag = "character",
                                            Time = "character"
                                          )
  )
  # Erstelle Liste der Stationen, die im jeweiligen Jahr aktiv waren.
  sdf_stations_per_year <- sdf_stations_per_year %>% group_by(Station)  %>% summarise()
  # Join mit Stationsdatei wegen Koordinaten
  sdf_stations_per_year <- inner_join(sdf_stations_per_year, sdf_stations, by=c("Station" = "Id"))
  # Mapping Koordinaten zu 5er und 10er Quadraten (Id)
  quad_size = 2.5
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Quad_2_5_Lat_Id = as.integer(Lat %/% quad_size + 90/quad_size)) # Normalisierung auf Ids >=0
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Quad_2_5_Long_Id = as.integer(Long %/% quad_size + 180/quad_size))
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Quad_2_5_Id = paste(Quad_2_5_Lat_Id, "-", Quad_2_5_Long_Id, sep=""))
  quad_size = 5
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Quad_5_Lat_Id = as.integer(Lat %/% quad_size + 90/quad_size))
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Quad_5_Long_Id = as.integer(Long %/% quad_size + 180/quad_size))
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Quad_5_Id = paste(Quad_5_Lat_Id, "-", Quad_5_Long_Id, sep=""))
  quad_size = 10
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Quad_10_Lat_Id = as.integer(Lat %/% quad_size + 90/quad_size))
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Quad_10_Long_Id = as.integer(Long %/% quad_size + 180/quad_size))
  sdf_stations_per_year <- sdf_stations_per_year %>% mutate(Quad_10_Id = paste(Quad_10_Lat_Id, "-", Quad_10_Long_Id, sep=""))
  write.table(sdf_stations_per_year, paste("E:\\Big Data Prak\\stations\\stations_", i, ".csv", sep=""), na='', quote=TRUE, sep=",", col.names=TRUE, row.names=FALSE)
  print(i)
}
# Lese Jahreslisten der Stations und zähle die (aktiven) Quadrate pro Jahr
df_quads <- NULL
year_start <- 1763
for(i in (year_start:2017)) {
  sdf_stations_per_year <- spark_read_csv(sc, "stations_year", 
                                          path = paste(path_stations_year,1863,".csv", sep = ""), 
                                          header = TRUE, 
                                          infer_schema = TRUE
  )
  count <- sdf_stations_per_year %>% group_by(Quad_10_Id) %>% summarise() #(count = n()))
  df_quads <- data.frame(c(i), c(count))
}
names(df_quads) <- c("Year", "Quad_Count")
ggplot(df_quads, aes(as.Date(Year, "%Y"), Quad_Count)) + geom_point()