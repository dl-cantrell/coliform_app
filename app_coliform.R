library(shiny)
library(tidyverse)
library(DBI)
library(DT)
library(lubridate)
library(shinyWidgets)
library(ggplot2)

library(dbplyr)
library(magrittr)
library(pool)
library(writexl)
library(shinycssloaders)
library(scales)
library(sf)
library(plyr)
library(leaflet)
#library(raster) #IMPORTANT: this package has conflicts with dplyr commands

#facility == "DST" only inside the query

# reactlog::reactlog_enable() #run this line in the console before launching the app
# shiny::reactlogShow() #run this line in the console AFTER running and then closing the app
# these two lines of code will give you the reactive graph for your app

source("connect_coliform.r") #how we are connecting to the SQL server
source("functions_coliform.r") #defines several functions we are using here

#some data sources we need for the map tab:
pws <- st_read("C:/Users/DCantrell/Desktop/pws_shapefiles/water_systems_all.shp")
counties <- st_read("C:/Users/DCantrell/Desktop/pws_shapefiles/ca_counties/CA_Counties.shp")
#Shapefile reprojection- want pws to match the priority area projections
counties_utm <- st_transform(counties, st_crs(pws))


# UI definition -----------------------------------------------------------
ui <- fluidPage(  #fluid page makes the pages dynamically expand to take up the full screen

  navbarPage(  "DBP Query Tool",
               
               # SRC JS helper functions
               tags$script(src = "helpers.js"),
               tags$head(
                 tags$link(rel = "stylesheet", type = "text/css", href = "app.css")
               ),
               theme = bslib::bs_theme(bootswatch = "cosmo"),
               # Home tab
               tabPanel(
                 "Home",
                 tags$h2("Welcome"),
                 tags$b("Descriptive text about how to use this tool goes here- a link to a data dictionary
           describing each column you can download from each tab should also go here")
               ),
               
               # DBP tab -----------------------------------------------------------------
               tabPanel(    
                 "DBP Results by Water System",
                 dateRangeInput(
                   "dbp_yr",
                   label = "Select Years:",
                   start = "2019-01-01", end = Sys.Date(),
                               ),
                 
                 fluidRow(
                   class = "searchPanel",
                   selectizeInput(
                     inputId = "sys_no",
                     label = "Enter PWS No.",
                     choices = NULL,
                     options = list(maxOptions = 8000) # Adjust based on expected size
                   ),
                   selectizeInput(
                     inputId = "sys_name",
                     label = "Enter PWS Name",
                     choices = NULL,
                     options = list(maxOptions = 8000) # Adjust based on expected size
                   )
                 ),
                 
                 
                 tags$b("Write a line or two here describing why an MCL exceedence may not be a violation, and to use the 
             violations tab to find actual violations"),
                 
                 fluidRow( 
                   actionButton(
                     inputId = "submit_dbp_yr_sys",
                     label = "Run Query"
                   )),
                 
                 tags$i("Must run query before downloading csv- queries may take a moment to complete"),
                 
                 # download buttons
                 fluidRow(
                   downloadButton("download_csv_dbp", "Download CSV")),
                 
                 conditionalPanel(
                   condition = "output.noDataMsg == true",
                   tags$h3("No data returned for this water system and date range")
                 ),
                 
                 tags$hr(),
                 withSpinner (plotOutput("ts_plot") ), # Add plotOutput for ggplot
                withSpinner( DT::dataTableOutput("dbp_mcl_table")), # with spinner makes a spinner go while the datatable is loading
                 tags$hr(),
                 
                 textOutput("selected_var")
               ),
               
               
               
               # DBP violations tab -----------------------------------------------------------------
               tabPanel(
                 "DBP Violations by Water System",
                 fluidRow(
                   class = "searchPanel",
                   selectizeInput(
                     inputId = "sys_no_vio",
                     label = "Enter PWS No.",
                     choices = NULL,
                     options = list(maxOptions = 8000) # Adjust based on expected size
                   ),
                   selectizeInput(
                     inputId = "sys_name_vio",
                     label = "Enter PWS Name",
                     choices = NULL,
                     options = list(maxOptions = 8000) # Adjust based on expected size
                   )
                 ),
                 
                 
                 fluidRow(
                   actionButton(
                     inputId = "submit_vio",
                     label = "Run Query"
                   ) ),
                 
                 tags$i("Must run query before downloading csv- queries may take a moment to complete"),
                 
                 # download buttons
                 fluidRow(
                   downloadButton("download_csv_vio", "Download CSV") ) ,
                 
                 tags$hr(),
                 conditionalPanel(
                   condition = "output.noDataMsgVio == true",
                   tags$h3("No data returned for this water system and date range")
                 ),
                withSpinner( plotOutput("vio_plot") ),
                withSpinner( DT::dataTableOutput("dbp_vio_table")), # with spinner makes a spinner go while the datatable is loading
                 
                 
                 textOutput("selected_var")  
                 
                 
               ),
               
               #map tab--------------------------------------------
               tabPanel( "Water System Numbers",
                        withSpinner (leafletOutput("map", width = "100%", height = "800px"))  #adjust height and width of plot here
               ) 
  )
)





#########################################################################################################################
# Server definition ----------------------------------------------------------------------------------------------------
##########################################################################################################################
server <- function(input, output, session) {
  
  
  # Initialize sys_no selectize input with server-side data
  observe({
    updateSelectizeInput(
      session, "sys_no",
      choices = water_systems()$water_system_no,
      server = TRUE
    )
  })
  
  # Initialize sys_name selectize input with server-side data
  observe({
    updateSelectizeInput(
      session, "sys_name",
      choices = water_systems()$water_system_name,
      server = TRUE
    )
  })
  
  
  # Synchronize sys_no/sys_name
  observeEvent(input$sys_no, {
    if (!is.null(input$sys_no) && input$sys_no != "") {
      sysname <- water_systems() %>%
        filter(water_system_no == input$sys_no) %>%
        pull(water_system_name)
      updateSelectizeInput(session, "sys_name", selected = sysname)
    }
  })
  
  observeEvent(input$sys_name, {
    if (!is.null(input$sys_name) && input$sys_name != "") {
      sysno <- water_systems() %>%
        filter(water_system_name == input$sys_name) %>%
        pull(water_system_no)
      updateSelectizeInput(session, "sys_no", selected = sysno)
    }
  })
  
  #Now for the violations tab, same as above (system number changes to reflect name choice and vice versa) --------------------------------------------
  # if sys_no was changed last, update NAME
  
  # Initialize sys_no selectize input with server-side data
  observe({
    updateSelectizeInput(
      session, "sys_no_vio",
      choices = water_systems()$water_system_no,
      server = TRUE
    )
  })
  
  # Initialize sys_name selectize input with server-side data
  observe({
    updateSelectizeInput(
      session, "sys_name_vio",
      choices = water_systems()$water_system_name,
      server = TRUE
    )
  })
  
  
  # Synchronize sys_no/sys_name
  observeEvent(input$sys_no_vio, {
    if (!is.null(input$sys_no_vio) && input$sys_no_vio != "") {
      sysname <- water_systems() %>%
        filter(water_system_no == input$sys_no_vio) %>%
        pull(water_system_name)
      updateSelectizeInput(session, "sys_name_vio", selected = sysname)
    }
  })
  
  observeEvent(input$sys_name_vio, {
    if (!is.null(input$sys_name_vio) && input$sys_name_vio != "") {
      sysno <- water_systems() %>%
        filter(water_system_name == input$sys_name_vio) %>%
        pull(water_system_no)
      updateSelectizeInput(session, "sys_no_vio", selected = sysno)
    }
  })
  
  # don't edit above this line for the server
  
  ######################################################################################################################
  # DBP MCLs
  yr_sys_dbp_mcls <- eventReactive(input$submit_dbp_yr_sys, {
    # Call the function to get data based on system and year input
    system <- input$sys_no
    #yr_for_func <- as.Date(input$dbp_yr)
    #date_range <- input$dbp_yr
    start_year <- input$dbp_yr[1]
    end_year <- input$dbp_yr[2]
    
    get_dbp_mcls(system, start_year, end_year)
  })
  
  output$dbp_mcl_table <- renderDataTable(
    yr_sys_dbp_mcls(),
    options = list(
      pageLength = 500
    )
  )
  
  # Download CSV handler for DBP data
  output$download_csv_dbp <- downloadHandler(
    filename = function() {
      paste("dbp_raw_data_", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      write.csv(yr_sys_dbp_mcls(), file, row.names = FALSE)
    }
  )
  
  # Reactive value to check if there is data
  output$noDataMsg <- reactive({
    nrow(yr_sys_dbp_mcls()) == 0
  })
  outputOptions(output, "noDataMsg", suspendWhenHidden = FALSE) 
  
  # plot time series
  output$ts_plot <- renderPlot({
    req(nrow(yr_sys_dbp_mcls()) > 0) # Ensure there are rows in the dataset
    ggplot(yr_sys_dbp_mcls(), aes(x = as.Date(collection_date), y = result, color = pscode)) +
      facet_wrap(~analyte) +
      geom_line() +
      geom_point() +
      scale_x_date(date_breaks = "1 month", date_labels = "%m/%y") +
      xlab("Month and Year")
  })
  
  
  
  ######################################################################################################################
  # DBP violations
  
  results_sys_vio <- eventReactive(input$submit_vio, {
    # Call the function to get data based on system and year input
    sys_vio <- input$sys_no_vio
    #  yr_for_func_vio <- ymd(input$vio_yr, truncated = 2L)
    get_vio(sys_vio)
  } )
  
  output$dbp_vio_table <- renderDataTable(
    results_sys_vio(),
    options = list(
      pageLength = 500
    )
  )
  
  # Download CSV handler for DBP Violations
  output$download_csv_vio <- downloadHandler(
    filename = function() {
      paste("dbp_violations_data_", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      write.csv(results_sys_vio(), file, row.names = FALSE)
    }
  )
  
  # Reactive value to check if there is data
  output$noDataMsgVio <- reactive({
    nrow(results_sys_vio()) == 0
  })
  outputOptions(output, "noDataMsgVio", suspendWhenHidden = FALSE)
  
  # plot time series
  output$vio_plot <- renderPlot({
    req(nrow(yr_sys_dbp_mcls()) > 0) # Ensure there are rows in the dataset
    ggplot(results_sys_vio(), aes(x = year ) ) +
      geom_bar() +
      labs(x = "year", y = "# of violations") +
      scale_x_continuous(breaks = pretty_breaks() ) 
  })
  
  
  ######################################################################################################################
  #Plot Map
  
  
  
  
  output$map <- renderLeaflet({

    leaflet() %>% 
      addTiles() %>%
      setMaxBounds( lng1 = -125.0
                    , lat1 = 28.5
                    , lng2 = -114.0
                    , lat2 = 44.0 ) %>%
      addPolygons(data = counties_utm,
                  fillColor = "transparent",
                  fillOpacity = 0,
                  color = "black",
                  layerId = ~NAME,  
                  popup = ~paste("County Name:", NAME)) %>% 
      # addProviderTiles("Stamen.Toner") %>% 
      addPolygons(data = pws, 
                  fillColor = "aliceblue", 
                  color = "darkblue",
                  layerId = ~pwsid,
                  popup = ~paste("PWSID:", pwsid))
 
  })
  
  outputOptions(output, "map", suspendWhenHidden = FALSE)  #important note- we need this option. otherwise, if you start to mess with 
                                                          #the first two tabs before the map is loaded, it messes up the loading of the map!!!
  
  
  
}



shinyApp(ui = ui, server = server)

