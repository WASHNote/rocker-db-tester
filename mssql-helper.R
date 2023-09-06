# library(dplyr) 
# library(dbplyr)
# library(odbc)
# library(DBI)
# library(rlist)

connect_mssql <- function(...) {
  dbConnect(odbc(),
            UID=.get_db_usr(), 
            PWD = .get_db_pwd(),
            driver = "{ODBC Driver 17 for SQL Server}",                       
            server=.get_db_svr(),                                           
            database=.get_db_nme(),
            encoding="latin1",
            ...
  )
}

disconnect_mssql <- function(con) {
  dbDisconnect(con)
} 

# seems like line breaks at the end of a cell can cause an error in the conversion string.
find_problematic_row <- function(df, table_name, find_cell = TRUE, find_character = TRUE) {
  n_splits = nrow(df)
  
  n <- nrow(df)
  max_rows <- floor(n / (n_splits))
  r <- rep(1:ceiling(n/max_rows),each=max_rows)[1:n]
  d <- split(df,r)
  
  target <- NULL
  found_row <- FALSE
  
  
  message("connecting")
  con <- connect_mssql()
  
  tryCatchLog({
    purrr::map(.x = d, function(y) {
      message("Checking a row out of ", length(d), " rows. ")
      target <<- y
      copy_to_mssql(con = con, df = y, tbl_name = table_name)
    })
  },
  error = function(error) {
    message("Error triggered", error)
    found_row <<- TRUE
  },
  finally = {
    message("disconnecting")
    disconnect_mssql(con)
    if (found_row) {
      message("Found problematic row")
      if (find_cell) {
        target_cell <- find_problematic_cell(prob_df = target, table_name = table_name, find_character)
        if (find_character) {
          return(list(target = target, target_cell = target_cell$cell, target_char = target_cell$char))
        }
        return(list(target = target, target_cell = target_cell))
      }
      return(target)
    }
  }
  )
  
  return(NULL)
}

find_problematic_cell <- function(prob_df, table_name, find_character = TRUE) {
  found_cell <- FALSE
  for (i in 1:ncol(prob_df))
  {
    tryCatchLog({
      message("connecting")
      con <- connect_mssql()
      copy_to_mssql(con = con, df = prob_df[,1:i], tbl_name = table_name)
    }, error = function(error) {
      message("Error triggered on cell", error)
      found_cell <<- TRUE
    },
    finally = {
      message("disconnecting")
      disconnect_mssql(con)
      if (found_cell) {
        message("Found problematic cell")
        if (find_character) {
          target_character <- find_problematic_character(prob_df = prob_df[,1:i], table_name = table_name)
          return(list(cell = prob_df[,i], char = target_character))
        }
        return(prob_df[,i])
      }
    })
  }
  return(NULL)
}

find_problematic_character <- function(prob_df, table_name) {
  
  test_seq <- function(x) {
    prob_df[1,ncol(prob_df)] <<- x
    
    message("Checking sequence: ", prob_df[1,ncol(prob_df)])
    
    found_seq <- FALSE
    tryCatchLog({
      message("connecting")
      con <- connect_mssql()
      copy_to_mssql(con = con, df = prob_df, tbl_name = table_name)
      
    }, error = function(error) {
      message("Error triggered on sequence ", error)
      found_seq <<- TRUE
    },
    finally = {
      message("disconnecting")
      disconnect_mssql(con)
      if (found_seq) {
        message("Found problematic sequence: ", x)
        return(TRUE)
      }
      return(FALSE)
    })
  }
  
  found_char <- FALSE
  found_seq <- FALSE
  
  prob_seq <- NULL
  
  string <- prob_df[[1,ncol(prob_df)]]
  
  message("Checking string ", string)
  
  while(!found_char&&!is.null(string)&&nchar(string)>1) {
    part1 = substr(string, 1,floor(nchar(string)/2))
    if (test_seq(part1)) {
      prob_seq <- part1
      string <- part1
    } else {
      part2 = substr(string, floor(nchar(string)/2)+1, nchar(string))
      if (test_seq(part2)) {
        prob_seq <- part2
        string <- part2
      } else {
        string <- NULL
      }
    }
    if (!is.null(string)&&nchar(string)==1) {
      found_char <- TRUE
      message("Found problematic character")
    }
  }
  
  return(list(char = string, seq = prob_seq))
}

unwanted_array = list(    'Š'='S', 'š'='s', 'Ž'='Z', 'ž'='z', 'À'='A', 'Á'='A', 'Â'='A', 'Ã'='A', 'Ä'='A', 'Å'='A', 'Æ'='A', 'Ç'='C', 'È'='E', 'É'='E',
                          'Ê'='E', 'Ë'='E', 'Ì'='I', 'Í'='I', 'Î'='I', 'Ï'='I', 'Ñ'='N', 'Ò'='O', 'Ó'='O', 'Ô'='O', 'Õ'='O', 'Ö'='O', 'Ø'='O', 'Ù'='U',
                          'Ú'='U', 'Û'='U', 'Ü'='U', 'Ý'='Y', 'Þ'='B', 'ß'='Ss', 'à'='a', 'á'='a', 'â'='a', 'ã'='a', 'ä'='a', 'å'='a', 'æ'='a', 'ç'='c',
                          'è'='e', 'é'='e', 'ê'='e', 'ë'='e', 'ì'='i', 'í'='i', 'î'='i', 'ï'='i', 'ð'='o', 'ñ'='n', 'ò'='o', 'ó'='o', 'ô'='o', 'õ'='o',
                          'ö'='o', 'ø'='o', 'ù'='u', 'ú'='u', 'û'='u', 'ü'='u', 'ý'='y', 'ý'='y', 'þ'='b', 'ÿ'='y' )

# Spent a lot of time because of character in a name of a column causing on a right truncation very seldomly so it seemed that the names were ok but actually they were causing HUGE problems in MSSQL
# https://stackoverflow.com/questions/20495598/replace-accented-characters-in-r-with-non-accented-counterpart-utf-8-encoding
fix_column_names <- function(df, drastic = FALSE) {
  x <- names(df)
  x <- str_replace_all(x, regex("\\W+"), " ")
  
  x <- chartr(paste(names(unwanted_array), collapse=''),
              paste(unwanted_array, collapse=''),
              x)
  
  #x <- iconv(x, to='ASCII//TRANSLIT')
  names(df) <- sapply(x, function(y) {
    #y <- iconv(iconv(enc2utf8(y), from="UTF-8", to="latin1", toRaw = TRUE), from="latin1", to="UTF-8")
    substring(y, 1, 128) #128 normally
  })
  
  names(df) <- chartr(".","_",names(as_tibble(df, .name_repair = "unique")))
  
  df
}

# Thanks to https://stackoverflow.com/questions/48105277/writing-unicode-from-r-to-sql-server
# Slightly modified / improved
copy_to_mssql <- function(con, df, tbl_name) {
  DBI::dbWriteTable(conn = con, 
                    name = SQL(tbl_name),
                    value = convert_to_UTF16_df(df),
                    overwrite = TRUE,
                    row.names = FALSE, 
                    field.types = mssql_field_types(df)
  )
}


insert_to_mssql <- function(con, df, tbl_name) {
  DBI::dbAppendTable(conn = con, 
                     name = SQL(tbl_name), 
                     value = convert_to_UTF16_df(df)
  )
}


# End of copied code


wait_approx <- function(seconds) {
  date_time<-Sys.time()
  while((as.numeric(Sys.time()) - as.numeric(date_time))<seconds){
    Sys.sleep(0.1)
  }
}

render_view <- function(view, view_name) {
  view_select <- view %>% sql_render() %>% as.character()
  message("Creating view: ", view_select)
  create_view(view_name, view_select)
}

# slicker would be to use a create statement directly from the select but not sure how for now as it may be database specific...
render_tbl <- function(tbl_reference, tbl_name) {
  tbl_data <- tbl_reference %>% collect()
  connect_repeat_query(query = function(con) {
    copy_to_mssql(con, tbl_data, tbl_name)
  })
}

# need to suppress the error message  on success.
create_view <- function(view_name, view_select) {
  connect_repeat_query(query = function(con) {
    tryCatchLog({
      tbl(con, view_name)
      DBI::dbExecute(conn = con, paste0("ALTER VIEW dbo.", view_name," AS ", view_select))
      message("Successfully altered view: ", view_name)
    }, error = function(e) {
      DBI::dbExecute(conn = con, paste0("CREATE VIEW dbo.", view_name," AS ", view_select))
      tbl(con, view_name)
      message("Successfully created new view: ", view_name)
    })
  })
}

connect_repeat_query <- function(query, max_repeats = 3, exponential_backoff = TRUE, seconds = 1, max_seconds = 120) {
  counter <- 0
  wait_time <- 0
  result = NULL
  while(is.null(result) & counter < max_repeats) {
    if (counter>0) {
      if (exponential_backoff) {
        # full jitter
        wait_time <- 2^(counter) * seconds * runif(1, 0, 1)
        
        # decorrelated jitter + full jitter - modification of the following
        # not yet tested
        # https://www.awsarchitectureblog.com/2015/03/backoff.html
        # wait_time <- min(2^(counter) * runif(1, seconds, max(seconds, wait_time) * 3), max_seconds)
      } else {
        wait_time <- seconds
      }
      message(paste0("Waiting ", wait_time," seconds before retry number ",counter,"."))
      wait_approx(wait_time)
    }
    tryLog({
      con <- connect_mssql()
      result = query(con)
      disconnect_mssql(con)
      return(result)
    }, include.full.call.stack = FALSE, include.compact.call.stack = FALSE)
    counter <- counter + 1
  }
  stop(paste0("Query did not complete within ", max_repeats," tries."))
}

copy_to_mssql_large <- function(df, tbl_name, ...) {
  connect_repeat_query(query = function(con) {
    copy_to_mssql(con, df = df[0,], tbl_name = tbl_name)
  })
  insert_to_mssql_large(df, tbl_name, ...)
}

insert_to_mssql_large <- function(df, tbl_name, mem_limit = 10000000, ...) {
  message(paste0("mem_limit = ", mem_limit))
  # get size of data frame
  # split into safe amount - based on one test assuming less than 3000000 bytes is ok (3840880 worked)
  # determine the number of splits
  n_splits <- as.numeric(ceiling(object.size(df)/mem_limit))+1
  message(paste0("n_splits = ", n_splits))
  n <- nrow(df)
  message(paste0("The large insert includes ", n, "rows"))
  max_rows <- floor(n / (n_splits)) + 1
  message(paste0("The maximum number of rows per insert is ", max_rows, "rows"))
  if (max_rows == 0) {
    stop("A single row exceeds the memory limit used. You can try to set a different memory limit with the argument mem_limit.",
         call. = FALSE)
  }
  # number of rows in final part - as a check
  n_last <- n %% max_rows
  
  r <- rep(1:ceiling(n/max_rows),each=max_rows)[1:n]
  d <- split(df,r)
  
  #### DEBIAN ONLY - need a system agnostic version in used
  # memfree <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", 
  #                              intern=TRUE))  
  # message(paste0("Memory available: ", memfree))
  
  purrr::map(.x = d, function(x) {
    
    if (object.size(x) > mem_limit) {
      message("Nesting inserts.")
      insert_to_mssql_large(x, tbl_name, mem_limit = mem_limit, ...)
    } else {
      message(paste0("Inserting ",nrow(x)," rows with ", object.size(x)," size."))
      connect_repeat_query(query = function(con) {
        insert_to_mssql(con, x, tbl_name)
      }, ...)
    }
    
  })
  
}


round_trip_encoding <- function(text, encoding = "UTF-16LE") {
  if (Encoding(text)=="unknown") {
    text16 <- iconv(
      enc2utf8(text), 
      from = "UTF-8", 
      to = encoding,
      toRaw = TRUE
    )
  } else {
    text16 <- iconv(
      text, 
      from = Encoding(text), 
      to = encoding,
      toRaw = TRUE
    )
  }
  iconv(
    text16, 
    from = encoding,
    to = "UTF-8"
  )
}

#################### Encoding functions
# Below are functions from online sources that are very helpful to overcome the encoding issues
# Thanks to https://stackoverflow.com/questions/48105277/writing-unicode-from-r-to-sql-server and the prior response: https://stackoverflow.com/a/61363450/7406873
# Using these helper functions was required to be able to write data to the SQL server due to the problem of different encoding issues around the odbc driver, R, and mssql. Required to convert UTF-8 character string into raw UTF-16LE bytes in tables to avoid truncation errors on mssql and nul byte errors on R.
# I made modification to an answer to address non-UTF8 encodings in R character vectors and posted the updated on stackoverflow

# see about changing this to not require the rlist package for list.cbind
convert_to_UTF16_df <- function(df){
  output <- cbind(df[sapply(df, typeof) != "character"]
                  , list.cbind(apply(df[sapply(df, typeof) == "character"], 2, function(x){
                    return(lapply(x, function(y) {
                      #e <- Encoding(y)
                      #print(e)
                      if (Encoding(y)=="unknown") {
                        unlist(iconv(enc2utf8(y), from = "UTF-8", to = "UTF-16LE", toRaw = TRUE))
                      } else {
                        unlist(iconv(y, from = Encoding(y), to = "UTF-16LE", toRaw = TRUE))
                      }
                    }))
                  }))
  )[colnames(df)]
  return(output)
}

# this is a dummy function to see if forcing text to latin1 will get rid of problematic sequences of characters
# convert_to_UTF16_df <- function(df) {
#   output <- cbind(df[sapply(df, typeof) != "character"]
#                   , list.cbind(apply(df[sapply(df, typeof) == "character"], 2, function(x){
#                     return(lapply(x, function(y) {
#                       # First send to latin1 and then back to hopefully get rid of bad characters for the driver/mssql on linux
#                       
#                       rawbom <- raw(2)
#                       rawbom[1] <- as.raw(255)
#                       rawbom[2] <- as.raw(254)
#                       
#                       
#                       # y <- round_trip_encoding(y, encoding = "latin1")
#                       # y2 <- unlist(iconv(y, from = "UTF-8", to = "UTF-16LE", toRaw = TRUE))
#                       
#                        #y <- iconv(iconv(enc2utf8(y), from="UTF-8", to="latin1", toRaw = TRUE), from="latin1", to="UTF-8")
#                       # if(nchar(y)>100) {
#                       #   message(y)
#                       # }
#                       
#                       #y <- paste0("AAAAAA ")
#                       
#                       if (Encoding(y)=="unknown") {
#                         y2 <- unlist(iconv(enc2utf8(y), from = "UTF-8", to = "UTF-16LE", toRaw = TRUE))
#                       } else {
#                         y2 <- unlist(iconv(y, from = Encoding(y), to = "UTF-16LE", toRaw = TRUE))
#                       }
#                       
#                       c(rawbom, y2)
#                       
#                     }))
#                   }))
#   )[colnames(df)]
#   return(output)
# }

field_details <- function(x) {
  details <-
    list(x=x,encoding=Encoding(x),bytes=nchar(x,"b"),chars=nchar(x,"c"),
         width=nchar(x,"w"),raw=paste(charToRaw(x),collapse=":"))
  print(t(as.matrix(details)))
}

convert_raw_UTF16_to_UTF8 <- function(raw_bytes) {
  # https://github.com/tidyverse/readr/issues/306
  # This is the text. 
  #text <- "1\t2\n"
  # Converted to UTF-16LE
  #text_utf16 <- iconv(text,from="UTF-8",to="UTF-16LE", toRaw = TRUE)
  
  # This is the Byte Order Mark:
  rawbom <- raw(2)
  rawbom[1] <- as.raw(255)
  rawbom[2] <- as.raw(254)
  
  tmp_file_name <- tempfile()
  fd <- file(tmp_file_name, "wb")
  
  # write BOM to top of file
  writeBin(rawbom, fd)
  
  lapply(raw_bytes, function(rb) {
    if (!is.null(rb)) {
      writeBin(unlist(rb), fd)
    } else {
      writeBin(unlist(iconv("-- NULL --",from="UTF-8",to="UTF-16LE", toRaw = TRUE)), fd)
    }
    writeBin(unlist(iconv("\n",from="UTF-8",to="UTF-16LE", toRaw = TRUE)), fd)
  })
  
  read.delim(tmp_file_name, header=FALSE, fileEncoding = "UTF-16LE", stringsAsFactors = FALSE)
}

mssql_field_types <- function(df){
  output <- list()
  output[colnames(df)[sapply(df, typeof) == "character"]] <- "nvarchar(max)"
  return(output)
}

show_query_and_collect <- function(dft){
  dft %>% show_query() %>% collect()
} 

all_equal_example <- function(dft, df) {
  dft %>% collect() %>% all_equal(df)
}

list_tables <- function(con) {
  DBI::dbListTables(con)
}
