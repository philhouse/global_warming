# Entsprechenden Path auskommentieren
#path_year = "E:\\Big Data Prak\\ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\by_year\\"
#path_stations = "E:\\Big Data Prak\\ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\ghcnd-stations.txt"
#path_year = "F:/Projekte/big_data_praktikum/by_year/"
#path_year = "D:/Entwicklung/big-data-praktikum/data/"

install.packages("xtable")
devtools::install_github("hadley/dplyr")
devtools::install_github("rstudio/sparklyr")
install.packages("ggplot2")
install.packages("rworldmap")
install.packages("rworldxtra")

library(sparklyr)
library(dplyr)
library(ggplot2)
library(rworldmap)
library(rworldxtra)

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
library(ggplot2)
ggplot(d, aes(as.Date(d$V2, "%Y%m%d"), d$V4)) + geom_point(aes(colour = d$station))

# Plotbeispiel GM Durchschnittstemp
dataf <- data %>% filter(country == "GM") %>% filter(V3 == "TMAX") %>% select(V2, V4) %>% group_by(V2) %>% summarise(temp = mean(V4/10))
d <- collect(dataf)
d %>% arrange(V2)
library(ggplot2)
ggplot(d, aes(as.Date(V2, "%Y%m%d"), temp)) + geom_point() + geom_smooth()

# Iteration Beispiel
data_count = vector("double", 21)
for(i in (1800:1820)) {
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
  sqlfunction(sc,"SELECT SUBSTR(V1,1,2) AS country,SUBSTR(V1,3,9) AS station,V2,V3,V4,V5,V6,V7,V8 FROM test WHERE (V6 IS NULL)") %>% invoke("createOrReplaceTempView", "test")
  data <- tbl(sc, "test")
  
  dataf <- data %>% filter(country == "IT") %>% filter(V3 == "TMAX") %>% select(country, V4) %>% group_by(country) %>% summarise(temp = mean(V4/10)) %>% collect
  data_count[i] = as.double(dataf[1,2])
  print(data_count[i])
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
data_stations <- read.fwf(path_stations, 
                        widths = c(11,9,10,7,3,31,4,4,6), 
                        header = FALSE,
                        comment.char='',
                        strip.white = TRUE)

data_stations %>% arrange(V2) #filter(V2 >= 85.0)

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

# Weltkarte mit Anstieg der Temperatur pro Land (oder 10°x10° Lat/Long Fläche)
# + Weltkarte für Sommer
# + Weltkarte für Winter

# Lineare Regression über die z. B. TAVG-Werte in GM aller Jahre (Geradenanstieg entspricht jährlichem Temperaturanstieg) 
# besser
# Temperatur Plot über alle Jahr
# Für jedes Jahr den Durchschnittswert bilden von TMIN, TMAX, TAVG und diese drei "Kurven" (Punkte) über alle Jahre plotten

# CO2 Plot überlegt mit globaler Temp. pro Jahr

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

# Plot der Eisschichtwerte in Antarktis (Keine FRTH-Werte in 2016, nur THIC in USA (Eisdicke auf Wasser))
# -