library(groundhog)
source('config.R')

version_string <- R.version$version.string

if (version_string == "R version 3.6.3 (2020-02-29)") {
  
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
  
  
} else {
  library(XML)
  library(dplyr) 
  library(dbplyr)
  library(odbc)
  library(DBI)
  library(rlist)
  
}

source('mssql-helper.R')

con <- connect_mssql()
tbl_list <- dbListTables(con)
tbl_result <- tbl(con, tbl_list[5])
