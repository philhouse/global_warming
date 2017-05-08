# Entsprechenden Path auskommentieren
#path = "E:\\Big Data Prak\\ftp.ncdc.noaa.gov\\pub\\data\\ghcn\\daily\\by_year\\1800.csv"
#path = ""

library(sparklyr)
library(dplyr)
sc <- spark_connect(master = "local", version = "2.1.0")
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

# Plotbeispiel f?r 1800er CSV
dataf <- data %>% filter(V3 == "TMAX") %>% select(V1, V2, V4)# %>% filter(V1 == "ASN00015643")
d <- collect(dataf)
d
library(ggplot2)
ggplot(d, aes(as.Date(d$V2, "%Y%m%d"), d$V4)) + geom_point(aes(colour = d$V1))


# Anzahl Messungen pro Land
data %>% filter(substr(V1, 1, 2) == "GM") %>% summarise(count = n())

# Anzahl Messstationen pro Land
data %>% filter(substr(V1, 1, 2) == "GM") %>% arrange(V1) %>% group_by(V1)  %>% summarise(count = n()) %>% summarise(count = n())

# Messarten
data %>% filter(substr(V1, 1, 2) == "GM") %>% group_by(V3) %>% summarise(count = n())



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

