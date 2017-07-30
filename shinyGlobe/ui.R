source("lang.R", encoding = "UTF-8")

library(leaflet)
library(shiny)

# Temperature select options 
vars_temp <- c(
  "Ganzes Jahr" = "time_span_year",
  "Sommerliches Halbjahr (4. Mai - 3. Nov.)" = "time_span_summer",
  "Winterliches Halbjahr (3. Nov. - 4. Mai)" = "time_span_winter"
)
# Define UI for application
shinyUI(fluidPage(
  # Tabs for interactive map and global plots
  navbarPage("Is the globe really warming?",
    tabPanel("Interaktive Karte",
             div(class="outer",
                 
                 tags$head(
                   # Include our custom CSS
                   includeCSS("style.css")
                   # includeScript("gomap.js")
                 ),
                 
                 # Map
                 leafletOutput("map", width="100%", height="100%"),
                 
                 # Sidepanel for settings for the values that we want to display
                 absolutePanel(id = "controls", class = "panel panel-default", fixed = TRUE,
                               draggable = TRUE, top = 60, left = "auto", right = 20, bottom = "auto",
                               width = 330, height = "auto",
                               
                               h2("Einstellungen"),
                               sliderInput("yearSlider",
                                           "Jahr:",
                                           min = 1917,
                                           max = 2016,
                                           value = 2016),
                               h3("Anzuzeigende Werte"),
                               checkboxInput("showTemp",label = s_temp, value = TRUE),
                               # checkboxInput("showCO2",label = s_emission, value = FALSE),
                               checkboxInput("showStorm",label = s_storm, value = FALSE),
                               checkboxInput("showPrec",label = s_prcp, value = FALSE),
                               # conditionalPanel("input.showTemp == true",
                               selectInput("time_span", "Zeitraum", vars_temp)
                               # )
                               
                               
                 )
             )
             ),
    tabPanel("Globale Plots",
             div(class="plots",
                 
                 tags$head(
                   # Include our custom CSS
                   includeCSS("style.css")
                 ),
                 # Plot
                 conditionalPanel(condition = "input.showTempPlot == true",
                                  conditionalPanel(condition = "input.time_span_plot == 'time_span_year'",
                                                   plotOutput("tempyPlot")
                                  ),
                                  conditionalPanel(condition = "input.time_span_plot == 'time_span_summer'",
                                                   plotOutput("tempsPlot")
                                  ),
                                  conditionalPanel(condition = "input.time_span_plot == 'time_span_winter'",
                                                   plotOutput("tempwPlot")
                                  )
                 ),
                 conditionalPanel(condition = "input.showCO2Plot == true",
                                  plotOutput("co2Plot")
                 ),
                 conditionalPanel(condition = "input.showPrecPlot == true",
                                  conditionalPanel(condition = "input.time_span_plot == 'time_span_year'",
                                                   plotOutput("precyPlot")
                                  ),
                                  conditionalPanel(condition = "input.time_span_plot == 'time_span_summer'",
                                                   plotOutput("precsPlot")
                                  ),
                                  conditionalPanel(condition = "input.time_span_plot == 'time_span_winter'",
                                                   plotOutput("precwPlot")
                                  )
                 ),
                 conditionalPanel(condition = "input.showStormPlot == true",
                                  conditionalPanel(condition = "input.time_span_plot == 'time_span_year'",
                                                   plotOutput("stormyPlot")
                                  ),
                                  conditionalPanel(condition = "input.time_span_plot == 'time_span_summer'",
                                                   plotOutput("stormsPlot")
                                  ),
                                  conditionalPanel(condition = "input.time_span_plot == 'time_span_winter'",
                                                   plotOutput("stormwPlot")
                                  )
                 ),
                 
                 # Sidepanel for settings for the values that we want to display
                 absolutePanel(id = "controls", class = "panel panel-default", fixed = TRUE,
                               draggable = TRUE, top = "auto", left = "auto", right = 20, bottom = 60,
                               width = 330, height = 300,
                               
                               h3("Anzuzeigende Werte"),
                               checkboxInput("showTempPlot",label = s_temp, value = TRUE),
                               checkboxInput("showCO2Plot",label = s_emission, value = FALSE),
                               checkboxInput("showPrecPlot",label = s_prcp, value = FALSE),
                               checkboxInput("showStormPlot",label = s_storm, value = FALSE),
                               selectInput("time_span_plot", "Zeitraum", vars_temp)
                               
                               
                 )
             )
             ),
    tabPanel("Weitere Informationen",
             # Detail Screen for explanation of our data
             div(tags$head(
                   # Include our custom CSS
                   includeCSS("style.css")
                 ),
                 class="container",
                 div(
                   h1("Ist der Klimawandel real?"),
                   
                   h2("Motivation"),
                   p(a(href="https://twitter.com/realdonaldtrump/status/265895292191248385?lang=de", 
                       "\"The concept of global warming was created by and for the Chinese in order to make U.S. manufacturing non-competitive.\""), 
                     ", so Donald Trump auf Twitter. Wir stellen seine Behauptung mit amerikanischen Datenquellen auf den Prüfstand."),
                   
                   h2("Datenquellen"),
                   
                   h3("Emissionsdaten"),
                   p("Der Datensatz der Emissionswerte beinhaltet jährliche Schätzungen des globalen Kohlenstoffausstoßes bei:",
                     tags$ul(
                       tags$li("Verbrennung fossiler Brennstoffe", tags$ul(
                         tags$li("Gase"), 
                         tags$li("Flüssigkeiten"),
                         tags$li("Feststoffe"))),
                       tags$li("Zementproduktion"), 
                       tags$li("Abfackelung")
                     )
                   ),
                   p(tags$b("Herausgeber:")), 
                   p(a(href = "http://cdiac.ornl.gov/", "Carbon Dioxide Information Analysis Center (CDIAC)"),
                     " im ", a(href = "https://www.ornl.gov/", "Oak Ridge National Laboratory (ORNL)"), 
                     " des ", a(href = "https://energy.gov/", "U.S. Department of Energy (DOE)")
                   ),
                   p(tags$b("Datenquellen:")), 
                   p("\"Global CO2 Emissions from Fossil-Fuel Burning, Cement Manufacture, and Gas Flaring: 1751-2014\", ", 
                     a(href="http://cdiac.ornl.gov/ftp/ndp030/global.1751_2014.ems", "http://cdiac.ornl.gov/ftp/ndp030/global.1751_2014.ems"), 
                     ", 03.05.2017"
                   ),
                   
                   h3("Wetterdaten"),
                   p("Bei den zugrundeliegenden Daten unserer Berechnungen handelt es sich um den ", 
                     a(href="https://www.ncdc.noaa.gov/ghcn-daily-description", "Global Historical Climatology Network (GHCN) Daily"), 
                     " Datensatz, welcher tägliche Messungen weltweit beinhaltet."
                   ),
                   p(tags$b("Herausgeber:")), 
                   p(a(href = "https://www.ncei.noaa.gov/", "National Centers for Environmental Information (NCEI)"), 
                     " der ", a(href = "http://www.noaa.gov/", "National Oceanic and Atmospheric Administration (NOAA)"), 
                     " des ", a(href = "https://www.commerce.gov/", "U.S. Department of Commerce (DOC)")
                   ),
                   p(tags$b("Datenquellen:")), 
                   p("GHCN-Daily Messdaten: ", 
                     a(href="ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/", "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/"), 
                     ", 02.05.2017"), 
                   p("GHCN-Daily Messstationen: ", 
                     a(href="ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt", "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"), 
                     ", 02.05.2017"
                   ),
                   
                   h2("Berechnungen"),
                   h3("Auswahl der Daten"),
                   p("Für die Berechnungen der CO2-Emissionen wurden die jährlichen \"Total\"-Werte verwendet, welche die Summe aller jährlichen Einzelwerte des Datensatzes sind."),
                   p("Die GHCN-Daily Messungen sind mit einem Flag zur Qualitätssicherung (QFLAG) versehen. 
                     Alle Messungen mit gesetztem QFLAG wurden nicht in unsere Berechnungen einbezogen, da dies auf einen fehlgeschlagenen Check hinweist. 
                     Des Weiteren wurden nur Daten ab 1917 verwendet und nicht alle Stationen. Mehr dazu im Abschnitt \"Vorgehen\"."),
                   p("Für die Berechnungen der Niederschlagsdifferenzen wurden die Messwerte für Niederschlag (PRCP) verwendet."),
                   p("Für die Berechnungen der Temperaturdifferenzen wurden die Messwerte für die maximalen Tagestemperaturen (TMAX) verwendet, 
                     da es für die durchschnittlichen Tagestemperaturen deutlich weniger Datensätze gab."),
                   p("Der Datensatz enthält 22 verschiedene Vorkommen von Wetterverhältnissen (engl. ", 
                     a(href = "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt", "Weather Types"), 
                     ", gelistet als WT**, wobei ** den Wettertyp als Zahl definiert). Folgende Typen zählten bei unseren Berechnungen als Unwetter:"
                   ),
                   tags$ul(
                     tags$li("02 = Heavy fog or heaving freezing fog"),
                     tags$li("03 = Thunder"),
                     tags$li("04 = Ice pellets, sleet, snow pellets, or small hail"),
                     tags$li("05 = Hail (may include small hail)"),
                     tags$li("07 = Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction"),
                     tags$li("10 = Tornado, waterspout, or funnel cloud"),
                     tags$li("11 = High or damaging winds"),
                     tags$li("16 = Rain (may include freezing rain, drizzle, and freezing drizzle)"),
                     tags$li("17 = Freezing rain"),
                     tags$li("18 = Snow, snow pellets, snow grains, or ice crystals")
                   ),
                   h3("Vorgehen"),
                   h4("Emissionsdaten"),
                   p("Der Datensatz der Emissionswerte besteht aus Schätzungen für Kohlenstoff. Wie vom Herausgeber selbst vorgeschlagen, 
                     wurden die Werte mit ", 
                     a(href = "http://cdiac.ornl.gov/ftp/ndp030/global.1751_2014.ems", "3,667"), 
                     " multipliziert, um die Werte für Kohlenstoffdioxid (CO2) zu erhalten."),
                   h4("Wetterdaten"),
                   h5("Aggregation"),
                   p("Um einen Überblick der Entwicklungen weltweit zu erhalten, wurden die Messdaten aggregiert. 
                     Zunächst wurden Stationen zusammengefasst zu Kacheln und dann wurden die Tageswerte einer Kachel zusammengefasst zu Jahreswerten. 
                     Die Kacheln entsprechen auch den Rechtecken auf der Weltkarte, welche die Temperaturwerte visualisieren."),
                   h6("Von Stationen zu Kacheln"),
                   p("Um die Stationen zu Kacheln zusammenzufassen, wurde die Weltkarte in Kacheln aus je 10 Breiten-/Höhengraden unterteilt. 
                     Die Messdaten aller Stationen innerhalb einer Kachel wurden dann aggregiert. 
                     Pro Kachel pro Tag wurde aus allen Messdaten ihrer Stationen der Durchschnitt gebildet. 
                     So wurden auch die Summe der Unwetter innerhalb einer Kachel durch die Anzahl ihrer Stationen geteilt. 
                     Dies ist nötig, da unterschiedliche Stationen das selbe Unwetter messen könnten. 
                     Die Anzahl der gemessenen Unwetter in einer Kachel steigt demnach potentiell mit der Anzahl ihrer Stationen."), 
                   p("Die Stationen innerhalb einer Kachel variieren von Jahr zu Jahr. Es können Stationen hinzukommen und wegfallen. 
                     Ein Beschränkung auf eine feste Menge von Stationen, die über alle 99 Jahre ausreichend Messdaten lieferten, 
                     hätte die Anzahl der Stationen und Messwerte zu sehr eingeschränkt. 
                     Es wurde daher die Annahme getroffen, dass alle Stationen innerhalb einer Kachel ähnliche Messwerte liefern. 
                     Unter dieser Vorraussetzung war es legitim die Menge der berücksichtigten Stationen in einer Kachel variabel zu halten."), 
                   h6("Von Tagen zu Jahren"),
                   p("Die gewonnenen aggregierten Tageswerte einer Kachel wurden anschließend vom jeweiligen Tageswert der Baseline subtrahiert. 
                     Für jede Kachel existiert ein eigenes Baseline-Jahr von Tageswerten. Eine genauere Beschreibung erfolgt im nächsten Kapitel. 
                     Pro Kachel wurde pro Jahr aus allen berechneten Tagesdifferenzen der Durchschnitt gebildet."),
                   h5("Baseline"),
                   p("Pro Kachel wurde eine Baseline bzw. ein Baseline-Jahr berechnet, welches Tageswerte (TMAX, PRCP und Unwetter) für ein ganzes Jahr enthält. 
                     In dem die Differenzen der Tageswerte zur Baseline berechnet wurden, wurden die jahreszeit-bedingten Schwankungen der Messwerte abgefangen."),
                   p("Ein Tageswert einer Baseline ist der Durchschnitt des jeweiligen Tageskachelwerts über 30 Jahre. 
                     Alle Baselines wurden über die Jahre 1917 bis einschließlich 1946 berechnet."), 
                   p("Es wurde zunächst eine Liste aller Kacheln erstellt, die im Jahr 1917 Messdaten lieferten. 
                     Für diese Kacheln wurde geprüft, ob sie im Zeitraum der 30 Jahre an mindestens 80% aller Tage mindestens einen der drei Messwerte lieferten. 
                     Für unsere Berechnungen wurden ausschließlich die Kacheln berücksichtigt, die diese Bedingungen erfüllten.")
               )
             )
    )
  )
))
