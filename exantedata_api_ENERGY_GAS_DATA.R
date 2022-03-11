#apt-get install -y r-base libcurl4-openssl-dev libxml2-dev

#install.packages("httr")
#install.packages("jsonlite")
#install.packages("xml2")
#install.packages("xts")
#install.packages("data.table")
library(data.table)
library(xts)
require(httr)
require(jsonlite)

base <- "https://apidata.exantedata.com/"
TOKEN <- ""


# -- BE SURE TO SET THE BELOW TO YOUR CREDENTIALS
username <- ""
password <- ""
#################################################


api.get.token <- function() {
  if (TOKEN == "") {
    getTokenCall <- POST(paste(base,"getToken", sep=""), body=list("username"=username, "password"=password), encode="json")
    if(status_code(getTokenCall) == 200) {
      TOKEN <<- content(getTokenCall,"parsed")$TOKEN
      TOKEN
    } else {
      print(content(getTokenCall,"parsed"))
    }
  } else {
    TOKEN
  }
}

api.get.data <- function(tickerQuery,startDate) {
  TOKEN <<- api.get.token()
  query <- list(
    "token"=TOKEN,
    "ticker"=tickerQuery,
    "startDate"=startDate
  )
  getDataCall <- POST(paste(base,"Data/Data",sep=""), body=query, add_headers(`Authorization` = paste("Bearer",TOKEN,sep=" ")), encode="json")
  if(status_code(getDataCall)==200) {
    content(getDataCall,"parsed")$DATA
  } else {
    print(content(getDataCall,"parsed"))
  }
}

cleaner <- function(data_to_clean,name) {
  tab <- as.data.table(unlist(data_to_clean),keep.rownames = TRUE)
  tab[, variable := name]
  tab[, date     := as.character(V1)]
  tab[, "V1" := NULL]
  names(tab)[names(tab) == "V2"] <- "value"
  setcolorder(tab, c("variable", "date", "value"))
  tab
}



### THIS IS YOUR MAIN FUNCTION, see https://apidocs.exantedata.com for further options.

getdata <- function(ticker,startDate) {
  #call to api
  apiOutput   <- api.get.data(ticker,startDate)
  # flatten list
  ticker_data <- lapply(apiOutput, '[')
  #save var names
  vars       <- names(ticker_data)
  # apply clean function
  dt_ls       <- mapply(ticker_data,FUN = cleaner,vars,SIMPLIFY = FALSE)
  #final output
  dt_ls
}

startDate <- '2022-01-01'
gas <- getdata('RU.NSGAS.GER.FLOWS.H',startDate)
## Downselect
df <- data.frame(gas[[1]])

## Warning, the dat is in UTC
df$date <- as.POSIXct(df$date,format="%Y-%m-%d %H:%M:%OS",tz="UTC")
gas.ts <- xts(df$value,df$date)
plot(window(gas.ts,start="2022-01-01"))

write.csv(df,"gas_hourly.csv")
     