# Entsprechenden Path auskommentieren
#path = "E:\\Big Data Prak\\ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\by_year\\1800.csv"
#path = "F:/Projekte/big_data_praktikum/by_year/2016.csv"
path = "D:/Entwicklung/big-data-praktikum/data/1800.csv"
library(sparklyr)
library(dplyr)
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
# DataFrame um L?ndercode erweitern, weil substr() zu teuer ist
# ggf. nur bestimmte Spalten laden


# Auf allen Jahren operieren



########################################
# Phil

# Weltkarte mit Stationen
# Weltkarte mit Anz. Stationen pro Land (als verschieden gro?e Punkte)



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

