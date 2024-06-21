library(shiny)
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

source("connect_coliform.r") #how we are connecting to the SQL server
source("functions_coliform.r") #defines several functions we are using here

#some data sources we need for the map tab:
pws <- st_read("C:/Users/DCantrell/Desktop/pws_shapefiles/water_systems_all.shp")
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
                 tags$h2("Welcome"),
                 tags$b("Descriptive text about how to use this tool goes here- a link to a data dictionary
           describing each column you can download from each tab should also go here")
               ),
               
               #coliform results tab ------------------------------------------------------------------------------------------
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
  
  # don't edit above this line for the server
  
  ######################################################################################################################
  # coliform results
  yr_sys_coli <- eventReactive(input$submit_coli_yr_sys, {
    # Call the function to get data based on system and year input
    system <- input$sys_no
    start_year <- input$coli_yr[1]
    end_year <- input$coli_yr[2]
    
    get_coli(system, start_year, end_year)
  })
  
  # Reactive value to store the clicked date
  clicked_date <- reactiveVal(NULL)
  
  # plot time series
  output$ts_plot <- renderPlotly({
    req(nrow(yr_sys_coli()) > 0) # Ensure there are rows in the dataset
    p <- ggplot(yr_sys_coli(), aes(x = as.Date(sample_date), fill = presence, text = paste("Date:", sample_date, "<br>Presence:", presence))) +
      scale_fill_manual(values = c("deepskyblue3", "red") ) +
      facet_wrap(~analyte_name) +
      geom_bar(stat = "count") +
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
}

shinyApp(ui, server)
  
  
  




shinyApp(ui = ui, server = server)

