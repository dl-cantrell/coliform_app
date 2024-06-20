library(odbc)
library(pool)
library(here)
library(RSQLite)

rm(list=ls())

#bacti <- dbPool(
 # drv = odbc(),
  #Driver = "SQL SERVER",
  #Server = "WB-GC-SQL-P04,1541",
 # App= "Microsoft Office 2010",
  #Trusted_Connection = "Yes",
  #Database = "DDWBactiLab")

sdwis_tdt  <- dbConnect(odbc(),
           Driver = "SQL Server",
           Server = "WB-GC-SQL-T02\\DDW",
           database = "SDWIS32")





#note: if you use the dbPool command, you won't be able to see the tables in the connections panel
#like you normally do when you make a SQL connection in R


#pdox <- dbPool(
 # drv = RSQLite::SQLite(), 
#  dbname = ("C:\\Users\\DCantrell\\Desktop\\tsasar shiny app\\pdox.db") )


