library(leaflet)
library(shiny)
library(jsonlite)
library(dplyr)
library(geojsonio)
library(ggplot2)

source("../init.r")
source("../emission.r")

data <- spark_read_csv(sc,name="weather_data",path = path_weather_data)

# Convert weather data to global data for plots
sdf_weather_data_global <- data %>% group_by(Year) %>%
  summarise(PRCP_year = mean(PRCP_year), PRCP_winter = mean(PRCP_winter), PRCP_summer = mean(PRCP_summer),
            TMAX_year = mean(TMAX_year), TMAX_winter = mean(TMAX_winter), TMAX_summer = mean(TMAX_summer),
            WTXX_year = mean(WTXX_year), WTXX_winter = mean(WTXX_winter), WTXX_summer = mean(WTXX_summer)) %>% arrange(Year)
sdf_co2_global <- read_co2_global(path_co2_global)
sdf_data_global <- left_join(sdf_weather_data_global, sdf_co2_global, by = "Year") %>% collect()

#Load GeoJSOn Data, Convert to sp to use polygon functions
geojson_polygon_data <- geojson_read("geojson_polygons_data.geojson", what="sp")

shinyServer(function(input, output) {
  
  observe({
    yearSlider <- input$yearSlider
    
    # Read Input Data for different Map Elements
    filtered_data <- data %>% filter(Year == yearSlider) %>% collect()
    
    # Convert into GeoJSON format for Circles on Map
    geojson_circle_data <- geojson_sp(geojson_json(filtered_data, lat='Lat', lon = 'Long'))
    geojson_polygon_filtered_data <- subset(geojson_polygon_data, Year == yearSlider)
    
    # Observer for temp/prec/storm timespan to determine which timespan of the year to look at
    observe({
      time_span = input$time_span
      if(time_span == "time_span_year"){
        temp_data = filtered_data$TMAX_year
        prec_data = geojson_circle_data$PRCP_year
        storm_data = geojson_circle_data$WTXX_year
      }else if(time_span == "time_span_summer"){
        temp_data = filtered_data$TMAX_summer
        prec_data = geojson_circle_data$PRCP_summer
        storm_data = geojson_circle_data$WTXX_summer
      }else if(time_span == "time_span_winter"){
        temp_data = filtered_data$TMAX_winter
        prec_data = geojson_circle_data$PRCP_winter
        storm_data = geojson_circle_data$WTXX_winter
      }
      
      temp_data = temp_data / 10
      prec_data = prec_data / 10
      
      # Convert into numeric for correct Calculation of Color Palette and removal of NAs
      temp_data_pal <- as.numeric(gsub("[^.0-9-]+","",temp_data))[!is.na(temp_data)]
      prec_data_pal <- as.numeric(gsub("[^.0-9-]+","",prec_data))[!is.na(prec_data)]
      storm_data_pal <- as.numeric(gsub("[^.0-9-]+","",storm_data))[!is.na(storm_data)]
      
      # Create Color Palette for temp, prec, storm
      tempPal = colorNumeric(c('#67a9cf','#f7f7f7','#ef8a62'), domain = temp_data_pal)
      precPal <- colorNumeric(palette = "Blues", domain = prec_data_pal)
      stormPal <- colorNumeric(palette = "Greens",domain = storm_data_pal)
      
      # Create Labels for our data output in Popups
      labels_temp <- sprintf("%s &#176;C", format(temp_data,digits = 2)) %>% lapply(htmltools::HTML)
      labels_prec <- sprintf("%s m&sup3;", format(prec_data,digits = 2)) %>% lapply(htmltools::HTML)
      labels_storm <- sprintf("%s Unwetter", format(storm_data,digits = 2)) %>% lapply(htmltools::HTML)
      
      # Generate Squares on the map for temperature
      leafletProxy("map", data = geojson_polygon_filtered_data) %>%
        clearShapes() %>%
        addPolygons(weight = 0.1,
                    smoothFactor = 0.2,
                    fillOpacity = 0.8,
                    fillColor = ~tempPal(temp_data),
                    highlightOptions = highlightOptions(
                      color = "white",
                      weight = 2,
                      bringToFront = TRUE),
                    label = labels_temp,
                    group = "temp")
      
      # Generate Circles on map for Precipitation
      leafletProxy("map", data = geojson_circle_data) %>%
        addCircles(radius = 200000,
                   stroke = FALSE,
                   fillColor = ~precPal(prec_data),
                   fillOpacity = 1,
                   label = labels_prec,
                   group = "prec")
      
      # Generate Circles on map for Storms
      leafletProxy("map", data = geojson_circle_data) %>%
        addCircles(radius = 300000,
                   stroke = FALSE,
                   fillColor = ~stormPal(storm_data_pal),
                   fillOpacity = 1,
                   label = labels_storm,
                   group = "storm")
      
      # Obeserve which Checkboxes Selected (which values to show) to decide which Legends to show
      observeEvent(input$map_groups,{
        activeGroup <- input$map_groups
        if("temp" %in% activeGroup){
          leafletProxy("map") %>%
            addLegend("bottomright", pal =tempPal, values = (temp_data_pal),
                      title = "Diff. Temperatur",
                      labFormat = labelFormat(suffix = "°C"),
                      opacity = 1,
                      layerId = "tempLegend")
        }
        if("prec" %in% activeGroup){
          leafletProxy("map") %>%
            addLegend("bottomright", pal =precPal, values = (prec_data_pal),
                      title = "Diff. Niederschlag",
                      labFormat = labelFormat(suffix = "mm"),
                      opacity = 1,
                      layerId = "precLegend")
        }
        if("storm" %in% activeGroup){
          leafletProxy("map") %>%
            addLegend("bottomright", pal =stormPal, values = storm_data_pal,
                      title = "Diff. Unwetter",
                      labFormat = labelFormat(suffix = " Anz."),
                      opacity = 1,
                      layerId = "stormLegend")
        }
      })
      
    })
    
  })
  
  # Leaflet Map generation
  output$map <- renderLeaflet({
    leaflet(data = geojson_10x10squares) %>%
      addTiles(
        urlTemplate = "//{s}.tiles.mapbox.com/v3/jcheng.map-5ebohr46/{z}/{x}/{y}.png",
        attribution = 'Maps by <a href="http://www.mapbox.com/">Mapbox</a>'
      ) %>%
      setView(lng = 0.00, lat = 20.00, zoom = 3)
  })
  
  
  # Observes the InputFields that determine which data to show (temp, prec, storms)
  observe({
    showTemp <- input$showTemp
    showPrec <- input$showPrec
    showStorm <- input$showStorm
    # showCO2 <- input$showCO2
    if(showTemp){
      leafletProxy("map") %>% showGroup("temp")
    }else {
      leafletProxy("map") %>% hideGroup("temp")
      leafletProxy("map") %>% removeControl("tempLegend")
    }
    if(showPrec){
      leafletProxy("map") %>% showGroup("prec")
    }else {
      leafletProxy("map") %>% hideGroup("prec")
      leafletProxy("map") %>% removeControl("precLegend")
    }
    if(showStorm){
      leafletProxy("map") %>% showGroup("storm")
    }else {
      leafletProxy("map") %>% hideGroup("storm")
      leafletProxy("map") %>% removeControl("stormLegend")
    }
  })
  
  
  # function that generates plots
  output$tempyPlot <- renderPlot({
    # plot(x = sdf_data_global$Year, y = sdf_data_global$TMAX_year, type="p")
    ggplot(sdf_data_global, aes(Year,TMAX_year)) + geom_smooth()
  })
  
  output$tempsPlot <- renderPlot({
    plot(x = sdf_data_global$Year, y = sdf_data_global$TMAX_summer, type="p")
  })
  
  output$tempwPlot <- renderPlot({
    plot(x = sdf_data_global$Year, y = sdf_data_global$TMAX_winter, type="p")
  })

  output$co2Plot <- renderPlot({
    plot(x = sdf_data_global$Year, y = sdf_data_global$co2, type="p")
  })

  output$precyPlot <- renderPlot({
    plot(x = sdf_data_global$Year, y = sdf_data_global$PRCP_year, type="p")
  })
  
  output$precsPlot <- renderPlot({
    plot(x = sdf_data_global$Year, y = sdf_data_global$PRCP_summer, type="p")
  })
  
  output$precwPlot <- renderPlot({
    plot(x = sdf_data_global$Year, y = sdf_data_global$PRCP_winter, type="p")
  })

  output$stormyPlot <- renderPlot({
    plot(x = sdf_data_global$Year, y = sdf_data_global$WTXX_year, type="p")
  })
  
  output$stormsPlot <- renderPlot({
    plot(x = sdf_data_global$Year, y = sdf_data_global$WTXX_summer, type="p")
  })
  
  output$stormwPlot <- renderPlot({
    plot(x = sdf_data_global$Year, y = sdf_data_global$WTXX_winter, type="p")
  })
})
