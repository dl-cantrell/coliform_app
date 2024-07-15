library(tidyverse)
library(DBI)
library(DT)
library(lubridate)
library(shinyWidgets)
library(ggplot2)
library(plotly) # Add this line

library(dbplyr)
library(magrittr)
library(pool)
library(writexl)
library(shinycssloaders)
library(scales)
library(sf)
library(plyr)
library(leaflet)

#facility == "DST" only inside the query

# reactlog::reactlog_enable() #run this line in the console before launching the app
# shiny::reactlogShow() #run this line in the console AFTER running and then closing the app
# these two lines of code will give you the reactive graph for your app

source("connect_coliform.R") #how we are connecting to the SQL server
source("functions_coliform.R") #defines several functions we are using here

#some data sources we need for the map tab:
pws <- st_read("C:/Users/DCantrell/Desktop/pws_shapefiles/water_systems/water_systems_all.shp")
counties <- st_read("C:/Users/DCantrell/Desktop/pws_shapefiles/ca_counties/CA_Counties.shp")
#Shapefile reprojection- want pws to match the priority area projections
counties_utm <- st_transform(counties, st_crs(pws))

#################################################################################################################
# UI definition -------------------------------------------------------------------------------------------------
#################################################################################################################
ui <- fluidPage(  #fluid page makes the pages dynamically expand to take up the full screen
  
  navbarPage(  "Coliform Data Query Tool",
               
               # SRC JS helper functions
               tags$script(src = "helpers.js"),
               tags$head(
                 tags$link(rel = "stylesheet", type = "text/css", href = "app.css")
               ),
               theme = bslib::bs_theme(bootswatch = "cosmo"),
               # Home tab
               tabPanel(
                 "Home",
                 tags$h2("Welcome!"),
                 tags$br(), #line break
                 tags$b("This is a tool built to run queries of the Safe Drinking Water Information System (SDWIS) database for the state of California.
                        Specifically, it will query coliform sample results for a user determined time period and water system.
                        It can also pull coliform violations results by water system. Data goes back to 1974 and up to the day before the query is ran."),
                 tags$br(), #line break
                 tags$br(), #line break
                 tags$b("Click on the tabs at the top of the page to toggle between coliform results by water system, coliform violations by water system, 
                        and an interactive map you can use to determine the name and number of water systems across the state."),
                 tags$br(), #line break
                 tags$br(), #line break
                 #create a direct download link
                 tags$a(href = "https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Fraw.githubusercontent.com%2Fdl-cantrell%2Fcoliform_app%2Fmain%2Fcoliform_data_dictionary.xlsx&wdOrigin=BROWSELINK",
                        
                        "Download Data Dictionary", 
                        target = "_blank",
                        download = "coliform_data_dictionary.xlsx")
               ),
               
#coliform results tab -----------------------------------------------------------------------------------------------------------------------------------------------------------------
               tabPanel(    
                 "Coliform Results by Water System",
                 dateRangeInput(
                   "coli_yr",
                   label = "Select Years:",
                   start = "2019-01-01", end = Sys.Date()
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
                     inputId = "submit_coli_yr_sys",
                     label = "Run Query"
                   )),
                 
                 tags$i("Must run query before downloading csv- queries may take a moment to complete"),
                 
                 # download buttons
                 fluidRow(
                   downloadButton("download_csv_coli", "Download CSV")),
                 
                 conditionalPanel(
                   condition = "output.noDataMsg == true",
                   tags$h3("No data returned for this water system and date range")
                 ),
                 
                 tags$hr(),
                 withSpinner(plotlyOutput("ts_plot")), # Update plotOutput to plotlyOutput
                 withSpinner(DT::dataTableOutput("coli_table")), # with spinner makes a spinner go while the datatable is loading
                 tags$hr(),
                 
                 textOutput("selected_var")
               ),


#coliform violations tab ------------------------------------------------------------------------------------------------------------------------------------------------------
tabPanel(
  "Coliform Violations by Water System",
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
  withSpinner( DT::dataTableOutput("coli_vio_table")), # with spinner makes a spinner go while the datatable is loading
  
  
  #textOutput("selected_var")  
  
  
),
               
#map tab-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
               tabPanel( "Water System Numbers",
                         tags$b("California water systems are regulated and tracked based on system classification and size. This is a single layer dataset for all 
                               drinking water systems in California that was created by consolidating different data sources for water systems of all sizes."),
                         tags$br(), #line break
                         tags$br(), #line break
                         tags$b("Information about how this layer was created:"),
                         tags$br(), #line break
                         tags$li( "The California Drinking Water System Area Boundaries (SABL) are publicly available. 
                         SABL is the database of record for public drinking water systems.
                         The locations are verified as the best available data and include over 4,000 water systems.
                         Collecting and verifying SABL boundaries is an ongoing effort."),
                         tags$li("There are public water systems (mostly non-community) that do not yet have a boundary in the SABL dataset. 
                         These systems have been geocoded from their Physical Locations in the State Drinking Water Information System (SDWIS) portal, 
                         with light data cleaning. Eventually these systems will be captured in SABL."),
                         tags$li("State Small Water Systems are non-public water systems that are regulated by County health officials in partnership 
                         with the State Water Board. These systems have been geocoded from their Physical Locations using the most recent County-provided data, 
                         with light data cleaning. Eventually, these systems may be found in SABL."),
                         tags$br(), #line break
                         tags$a(href = "https://gispublic.waterboards.ca.gov/portal/home/item.html?id=346d649d1e654737ac5b6855466e89b2", 
                                "Original layer hosted by the waterboards public GIS layers page", target = "_blank"),
                         withSpinner (leafletOutput("map", width = "100%", height = "800px"))  #adjust height and width of plot here
               ) 
  ) )


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
  # coliform results
  yr_sys_coli <- eventReactive(input$submit_coli_yr_sys, {
    # Call the function to get data based on system and year input
    system <- input$sys_no
    start_date <- input$coli_yr[1]
    end_date <- input$coli_yr[2]
    
     get_coli(system, start_date, end_date)
  })
  
  # Reactive value to store the clicked date
  clicked_date <- reactiveVal(NULL)
  
  # plot time series
  output$ts_plot <- renderPlotly({
    req(nrow(yr_sys_coli()) > 0) # Ensure there are rows in the dataset
    
    p <- ggplot(yr_sys_coli(), aes(x = as.Date(sample_date), 
                    fill = presence, text = paste("Date:", sample_date, "<br>Presence:", presence))) +
      scale_fill_manual(values = c("deepskyblue3", "red") ) +
      facet_wrap(~analyte_name) +
      geom_bar(width = 12, stat = "count" ) + # Adjust the width here
      scale_x_date(date_breaks = "1 month", date_labels = "%m/%y") +
      xlab("Month and Year") +
      ylab("Count")
    
    ggplotly(p, tooltip = "text") %>%
      event_register("plotly_click")
  })
  
  observeEvent(event_data("plotly_click"), {
    click_data <- event_data("plotly_click")
    req(click_data)
    
    clicked_date(as.Date(click_data$x))
  })
  
  output$coli_table <- renderDataTable({
    data <- yr_sys_coli()
    highlight_date <- clicked_date()
    
    if (!is.null(highlight_date)) {
      data <- data %>%
        mutate(highlight = ifelse(as.Date(sample_date) == highlight_date, "highlight", ""))
    } else {
      data$highlight <- ""
    }
    
    datatable(data, options = list(pageLength = 500), rownames = FALSE) %>%
      formatStyle(
        'highlight',
        target = 'row',
        backgroundColor = styleEqual(c("highlight", ""), c("yellow", ""))
      )
  })
  
  # Download CSV handler for coliform data
  output$download_csv_coli <- downloadHandler(
    filename = function() {
      paste("coliform_raw_data_", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      write.csv(yr_sys_coli(), file, row.names = FALSE)
    }
  )
  
  # Reactive value to check if there is data
  output$noDataMsg <- reactive({
    nrow(yr_sys_coli()) == 0
  })
  outputOptions(output, "noDataMsg", suspendWhenHidden = FALSE) 
  
  
  ######################################################################################################################
  # Coliform violations
  
  results_sys_vio <- eventReactive(input$submit_vio, {
    # Call the function to get data based on system and year input
    sys_vio <- input$sys_no_vio
    yr_for_func_vio <- ymd(input$vio_yr, truncated = 2L)
    get_vio(sys_vio)
  } )
  
  output$coli_vio_table <- renderDataTable(
    results_sys_vio(),
    options = list(
      pageLength = 500
    )
  )
  
  # Download CSV handler for DBP Violations
  output$download_csv_vio <- downloadHandler(
    filename = function() {
      paste("coli_violations_data_", Sys.Date(), ".csv", sep = "")
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
    req(nrow(results_sys_vio()) > 0) # Ensure there are rows in the dataset
    
    ggplot(results_sys_vio(), aes(x = year ) ) +
      geom_bar() +
      labs(x = "year", y = "# of violations") +
      scale_x_discrete(breaks = pretty_breaks() ) 
    
    
    
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
                  popup = ~paste("PWS_ID:", pwsid, "<br>", "PWS_name:", system_nam))
    
  })
  
  outputOptions(output, "map", suspendWhenHidden = FALSE)  #important note- we need this option. otherwise, if you start to mess with 
  #the first two tabs before the map is loaded, it messes up the loading of the map!!!
  
  
  
}

shinyApp(ui, server)







shinyApp(ui = ui, server = server)
