source("lang.R", encoding = "UTF-8")
source("../init.r")
source("../emission.r")
init()

data <- spark_read_csv(sc,name="weather_data",path = path_weather_data)

# Convert weather data to global data for plots
sdf_weather_data_global <- data %>% group_by(Year) %>%
  summarise(PRCP_year = mean(PRCP_year) / 10, PRCP_winter = mean(PRCP_winter) / 10, PRCP_summer = mean(PRCP_summer) / 10,
            TMAX_year = mean(TMAX_year) / 10, TMAX_winter = mean(TMAX_winter) / 10, TMAX_summer = mean(TMAX_summer) / 10,
            WTXX_year = mean(WTXX_year), WTXX_winter = mean(WTXX_winter), WTXX_summer = mean(WTXX_summer)) %>% arrange(Year)
sdf_co2_global <- read_co2_global(path_co2_global)
sdf_data_global <- left_join(sdf_weather_data_global, sdf_co2_global, by = "Year") %>% collect()

shinyServer(function(input, output) {
  
  observe({
    yearSlider <- input$yearSlider
    
    # Read Input Data for different Map Elements
    filtered_data <- data %>% filter(Year == yearSlider) %>% collect()
    
    # Convert into GeoJSON format for Circles on Map
    geojson_circle_data <- geojson_sp(geojson_json(filtered_data, lat='Lat', lon = 'Long'))
    #Load yearly GeoJSOn Data, Convert to sp to use polygon functions
    geojson_polygon_filtered_data <- geojson_read(paste(path_processed,"polygons_weather_data\\geojson_polygons_data_",yearSlider,".geojson",sep=""), what="sp")
    
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
      # temp_data_pal <- as.numeric(gsub("[^.0-9-]+","",temp_data))[!is.na(temp_data)]
      # prec_data_pal <- as.numeric(gsub("[^.0-9-]+","",prec_data))[!is.na(prec_data)]
      storm_data_pal <- as.numeric(gsub("[^.0-9-]+","",storm_data))[!is.na(storm_data)] * 100
      
      redToBlueColors <- c('#67001f','#b2182b','#d6604d','#f4a582','#fddbc7','#f7f7f7','#d1e5f0','#92c5de','#4393c3','#2166ac','#053061')
      # Color Palette Domains
      tempPalDomain <- c(-12,12)
      precPalDomain <- c(-20,20)
      stormPalDomain <- c(-20,20)
      
      # Create Color Palette for temp, prec, storm
      tempPal <- colorNumeric(redToBlueColors, domain = tempPalDomain, reverse = TRUE)
      precPal <- colorNumeric(redToBlueColors, domain = precPalDomain, reverse = TRUE)
      # precPal <- colorNumeric(c('#a50026','#d73027','#f46d43','#fdae61','#fee090','#ffffbf','#e0f3f8','#abd9e9','#74add1','#4575b4','#313695'), domain = precPalDomain, reverse = FALSE)
      stormPal <- colorNumeric(c('#40004b','#762a83','#9970ab','#c2a5cf','#e7d4e8','#f7f7f7','#d9f0d3','#a6dba0','#5aae61','#1b7837','#00441b'),domain = stormPalDomain, reverse = TRUE)
      
      # Create Labels for our data output in Popups
      labels_temp <- sprintf("%s &#176;C", format(temp_data,digits = 2)) %>% lapply(htmltools::HTML)
      labels_prec <- sprintf("%s  mm", format(prec_data,digits = 2)) %>% lapply(htmltools::HTML)
      labels_storm <- sprintf("%s Unwetter", storm_data_pal) %>% lapply(htmltools::HTML)
      
      # Generate Squares on the map for temperature
      leafletProxy("map", data = geojson_polygon_filtered_data) %>%
        clearShapes() %>%
        addPolygons(weight = 0.1,
                    smoothFactor = 0.2,
                    fillOpacity = 0.8,
                    fillColor = ~tempPal(temp_data),
                    # highlightOptions = highlightOptions(
                    #   color = "white",
                    #   weight = 2,
                    #   bringToFront = TRUE),
                    label = labels_temp,
                    group = "temp")

      # Generate Circles on map for Storms
      leafletProxy("map", data = geojson_circle_data) %>%
        addCircles(radius = 300000,
                   stroke = FALSE,
                   fillColor = ~stormPal(storm_data_pal),
                   fillOpacity = 1,
                   label = labels_storm,
                   group = "storm")
      
      # Generate Circles on map for Precipitation (last because it is the smallest circle and is seen at any time)
      leafletProxy("map", data = geojson_circle_data) %>%
        addCircles(radius = 200000,
                   stroke = FALSE,
                   fillColor = ~precPal(prec_data),
                   fillOpacity = 1,
                   label = labels_prec,
                   group = "prec")
      
      # Obeserve which Checkboxes Selected (which values to show) to decide which Legends to show
      observeEvent(input$map_groups,{
        activeGroup <- input$map_groups
        if("temp" %in% activeGroup){
          leafletProxy("map") %>%
            addLegend("bottomright", pal =tempPal, values = tempPalDomain,
                      title = s_temp_legend,
                      labFormat = labelFormat(suffix = " °C"),
                      opacity = 1,
                      layerId = "tempLegend")
        }
        if("prec" %in% activeGroup){
          leafletProxy("map") %>%
            addLegend("bottomright", pal =precPal, values = precPalDomain,
                      title = s_prcp_legend,
                      labFormat = labelFormat(suffix = " mm"),
                      opacity = 1,
                      layerId = "precLegend")
        }
        if("storm" %in% activeGroup){
          leafletProxy("map") %>%
            addLegend("bottomright", pal =stormPal, values = stormPalDomain,
                      title = s_storm_legend,
                      labFormat = labelFormat(suffix = " Anz."),
                      opacity = 1,
                      layerId = "stormLegend")
        }
      })
      
    })
    
  })
  
  # Leaflet Map generation
  output$map <- renderLeaflet({
    leaflet() %>%
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
    ggplot(sdf_data_global, aes(Year,TMAX_year)) +
      xlab(s_year) + ylab(s_temp) + ggtitle(generate_plot_title(s_temp, s_year)) +
      geom_point() + geom_smooth() +
      theme(axis.text=element_text(size=12), axis.title=element_text(size=14), title = element_text(size=14))
  })
  
  output$tempsPlot <- renderPlot({
    ggplot(sdf_data_global, aes(Year,TMAX_summer)) +
      xlab(s_year) + ylab(s_temp) + ggtitle(generate_plot_title(s_temp, s_summer)) +
      geom_point() + geom_smooth() +
      theme(axis.text=element_text(size=12), axis.title=element_text(size=14), title = element_text(size=14))
  })
  
  output$tempwPlot <- renderPlot({
    ggplot(sdf_data_global, aes(Year,TMAX_winter)) + 
      xlab(s_year) + ylab(s_temp) + ggtitle(generate_plot_title(s_temp, s_winter)) +
      geom_point() + geom_smooth() +
      theme(axis.text=element_text(size=12), axis.title=element_text(size=14), title = element_text(size=14))
  })

  output$co2Plot <- renderPlot({
    ggplot(sdf_data_global, aes(Year,Total_in_mega_tons)) +
      xlab(s_year) + ylab("CO2-Ausstoß (Mt)") + ggtitle("Entwicklung des globalen, geschätzten CO2-Ausstoßes (Mt)") +
      geom_point() + geom_smooth() +
      theme(axis.text=element_text(size=12), axis.title=element_text(size=14), title = element_text(size=14))
  })

  output$precyPlot <- renderPlot({
    ggplot(sdf_data_global, aes(Year,PRCP_year)) +
      xlab(s_year) + ylab(s_prcp) + ggtitle(generate_plot_title(s_prcp, s_year)) +
      geom_point() + geom_smooth() +
      theme(axis.text=element_text(size=12), axis.title=element_text(size=14), title = element_text(size=14))
  })
  
  output$precsPlot <- renderPlot({
    ggplot(sdf_data_global, aes(Year,PRCP_summer)) +
      xlab(s_year) + ylab(s_prcp) + ggtitle(generate_plot_title(s_prcp, s_summer)) +
      geom_point() + geom_smooth() + 
      theme(axis.text=element_text(size=12), axis.title=element_text(size=14), title = element_text(size=14))
  })
  
  output$precwPlot <- renderPlot({
    ggplot(sdf_data_global, aes(Year,PRCP_winter)) +
      xlab(s_year) + ylab(s_prcp) + ggtitle(generate_plot_title(s_prcp, s_winter)) +
      geom_point() + geom_smooth() +
      theme(axis.text=element_text(size=12), axis.title=element_text(size=14), title = element_text(size=14))
  })

  output$stormyPlot <- renderPlot({
    ggplot(sdf_data_global, aes(Year,WTXX_year)) +
      xlab(s_year) + ylab(s_storm) + ggtitle(generate_plot_title(s_storm,s_year, s_storm_detail)) +
      geom_point() + geom_smooth() +
      theme(axis.text=element_text(size=12), axis.title=element_text(size=14), title = element_text(size=14))
  })
  
  output$stormsPlot <- renderPlot({
    ggplot(sdf_data_global, aes(Year,WTXX_summer)) +
      xlab(s_year) + ylab(s_storm) + ggtitle(generate_plot_title(s_storm, s_summer, s_storm_detail)) +
      geom_point() + geom_smooth() +
      theme(axis.text=element_text(size=12), axis.title=element_text(size=14), title = element_text(size=14))
  })
  
  output$stormwPlot <- renderPlot({
    ggplot(sdf_data_global, aes(Year,WTXX_winter)) +
      xlab(s_year) + ylab(s_storm) + ggtitle(generate_plot_title(s_storm, s_winter, s_storm_detail)) +
      geom_point() + geom_smooth() +
      theme(axis.text=element_text(size=12), axis.title=element_text(size=14), title = element_text(size=14))
  })
})

generate_plot_title = function(s_measure, s_time, s_measure_detail = "") {
  if (s_measure_detail != "") s_measure_detail <- paste(" ", s_measure_detail, sep="")
  paste(paste("Entwicklung der globalen ", s_measure, s_measure_detail, " pro ", s_time,sep=""), "im Vergleich zur Baseline (Durchschnittswerte der Jahre 1917-1946)", sep="\n")
}