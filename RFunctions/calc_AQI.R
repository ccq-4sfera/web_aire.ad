library(readxl)

# Convert hourly data from NO2, SO2, O3 and PM10 and PM2.5 24h running mean to EU index range
# metadata of EU index range
calc_AQI <- function(df,poll.list,suffix=NULL,keep_all=F){
  ref_index <- read_excel("/var/rprojects/RFunctions/indexAD.xlsx")
  # ref_index <- filter(ref_index, country == 'EU')
  
  # change pm10run->PM10, pm2.5run->PM2.5
  ref_index[ref_index$pol=="pm10run","pol"] <- "PM10"
  ref_index[ref_index$pol=="pm2.5run","pol"] <- "PM2.5"
  
  for(p in poll.list){
    index_pol <- ref_index %>% dplyr::filter(pol %in% p)
  
    index_range <- list(index_pol$a, index_pol$b, index_pol$c, index_pol$d,
                        index_pol$e, index_pol$f, index_pol$g)
    
    df <- df %>% dplyr::rename(poll=paste0(p,suffix))
    
    index_num <- list(1,2,3,4,5,6)
    
    # create a new variable from mean
    df <- df %>% mutate(index=cut(poll, breaks=index_range, labels=index_num, include.lowest = TRUE))
    
    
    colnames(df)[which(names(df) == "poll")] <- paste0(p,suffix)
    colnames(df)[which(names(df) == "index")] <- paste0("index_",p)
  }
  
  if(!keep_all){
    index <- df %>% select(c(date,contains("index")))
  }
  
  return(index)
}


#With decimals
calc_AQI_dec <- function(df,poll.list,suffix=NULL){
  ref_index <- read_excel("/var/rprojects/RFunctions/indexAD.xlsx")
  # ref_index <- filter(ref_index, country == 'EU')
  
  for(p in poll.list){
    if(p %in% ref_index$pol){
      index_pol <- ref_index %>% dplyr::filter(pol %in% p)
      
      index_range <- list(index_pol$a, index_pol$b, index_pol$c, index_pol$d,
                          index_pol$e, index_pol$f, index_pol$g)
      
      df <- df %>% dplyr::rename(poll=paste0(p,suffix))
      
  
      # calc index by assigning integer to range number
      df <- df %>% mutate(int_index=cut(poll, breaks=index_range, labels=FALSE, include.lowest = TRUE))
      
      # r1, r2 are ranges just below and above of the value of pollutant
      # decimals: if poll=r1, decimals=0; if poll=r2, decimals=1
      df$r1 = as.numeric(ifelse(!is.na(df$int_index),index_range[df$int_index],NA))
      df$r2 = as.numeric(ifelse(!is.na(df$int_index),index_range[df$int_index+1],NA))
      
      df <- df %>% mutate(add_decimals = (poll-r1)/(r2-r1))
      
      # df$add_decimals = (df$poll-r1)/(r2-r1)
      df$int_index <- as.numeric(df$int_index)
      
      # df$index = df$int_index+df$add_decimals
      
      df <- df %>% mutate(index=int_index+add_decimals)
      
      colnames(df)[which(names(df) == "poll")] <- paste0(p,suffix)
      colnames(df)[which(names(df) == "index")] <- paste0("index_",p)
    }
  }
  
  if("date" %in% colnames(df)){
    indexes <- df %>% select(c(date,starts_with("index")))
  }else if("StationCode" %in% colnames(df)){ #FIXME:should not be else if, should be if StationCode (and date) in colnames
    indexes <- df %>% select(c(StationCode,starts_with("index")))
    # indexes <- df %>% select(c(date,StationCode,starts_with("index")))
  }else{
    indexes <- df
  }
  if("station" %in% colnames(df)){
    indexes <- df %>% select(c(date,station,starts_with("index")))
  }  
  
  return(indexes)
}