# MRAN has shut down their service of snapshots - switch to groundhog

#.libPaths()
#options(repos = c(CRAN = "https://mran.microsoft.com/snapshot/2020-04-24"))

# you may need to create ~/.Renviron with the following line but replacing the verison number
# R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/3.63' 

# turns out the ODBC package is not available in some snapshots that are older - so using the latest CRAN source and building - probably less error prone anyway

# Tested so far on R 3.63
options(repos = c(CRAN = "https://cran.rstudio.com"))

# THIS DID NOT WORK
# install.packages("Rcpp", type = "source")
# install.packages("rlang", type = "source")
# install.packages("odbc", type = "source")

# See: https://www.brodrigues.co/blog/2023-01-12-repro_r/

install.packages("groundhog")

options(repos = c(CRAN = 'https://cran.rstudio.com'))
library("groundhog")

#older
groundhog.library("library(XML)","2020-01-18")

#newer
groundhog.library("library(XML)","2020-07-06", tolerate.R.version = '3.6.3')

groundhog.library(
  "library(dplyr) 
  library(dbplyr)
  library(odbc)
  library(DBI)
  library(rlist)",
  "2020-04-24", ignore.deps = "XML")

source('mssql-helper.R')

con <- connect_mssql()

db_list_tables(con)

tbl(con, db_list_tables(con)[5])

