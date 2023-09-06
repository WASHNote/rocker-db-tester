library(groundhog)
source('config.R')

version_string <- R.version$version.string

if (version_string == "R version 3.6.3 (2020-02-29)"){
  
  options(repos = c(CRAN = 'https://cran.rstudio.com'))
  
  #older
  groundhog.library("library(XML)","2020-01-18")
  
  #newer - not needed
  #groundhog.library("library(XML)","2020-07-06", tolerate.R.version = '3.6.3')
  
  groundhog.library(
    "  library(dplyr) 
  library(dbplyr)
  library(odbc)
  library(DBI)
  library(rlist)",
    "2020-04-24", ignore.deps = "XML")
  
  source('mssql-helper.R')
  
  con <- connect_mssql()
  db_list_tables(con)
  tbl(con, db_list_tables(con)[5])
  
  
} else {
  cat("You are not running R version 3.6.3.\n")
}