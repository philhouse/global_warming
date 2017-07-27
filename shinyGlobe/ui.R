library(leaflet)
library(shiny)

# Temperature select options 
vars_temp <- c(
  "Ganzes Jahr" = "time_span_year",
  "Sommer" = "time_span_summer",
  "Winter" = "time_span_winter"
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
                                           # min = 1763,
                                           min = 1917,
                                           max = 2016,
                                           value = 2016),
                               h3("Anzuzeigende Werte"),
                               checkboxInput("showTemp",label = "Temperatur", value = TRUE),
                               # checkboxInput("showCO2",label = "CO2", value = FALSE),
                               checkboxInput("showPrec",label = "Niederschlag", value = FALSE),
                               checkboxInput("showStorm",label = "Unwetter", value = FALSE),
                               # conditionalPanel("input.showTemp == true",
                               selectInput("time_span", "Zeitraum", vars_temp)
                               # )
                               
                               
                 )
             )
             ),
    tabPanel("Globale Plots",
             div(class="outer",
                 
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
                               width = 330, height = "auto",
                               
                               h3("Anzuzeigende Werte"),
                               checkboxInput("showTempPlot",label = "Temperatur", value = TRUE),
                               checkboxInput("showCO2Plot",label = "CO2", value = FALSE),
                               checkboxInput("showPrecPlot",label = "Niederschlag", value = FALSE),
                               checkboxInput("showStormPlot",label = "Unwetter", value = FALSE),
                               selectInput("time_span_plot", "Zeitraum", vars_temp)
                               
                               
                 )
             )
             )
  )
))
