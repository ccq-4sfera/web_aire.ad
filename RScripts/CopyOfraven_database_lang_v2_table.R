# Connect to RAVEN database to download observations, calculate AQIndex etc
# Author: Cristina Carnerero
# Date: 16/12/2021

#TODO null: -9900

#TODO filtrar al calendari quan la mobil no estigui operativa


update_raven <- function(){
  ptm <- proc.time() #start counting time
  
  library(RPostgres)
  library(DBI)
  library(tidyr)
  library(openxlsx)
  library(plyr)
  library(dplyr)
  library(openair)
  library(httr)
  library(lubridate)
  
  # PARAMETERS ----
  # DB Raven
  database = "ravendb"
  host = "localhost"
  port = 5432
  user = "ravendb"
  pwd = "ravendb"
  
  # Others
  Andorra_info_file = "/var/rprojects/Projects/AireAD/Andorra_info.xlsx"
  AD_hourly_RDS = "/var/rprojects/DATA/Andorra_hourly.RDS"
  # AD_hourly_csv = "/var/rprojects/DATA/Andorra_hourly.csv"
  tweets_path = "/var/rprojects/Projects/AireAD/Tweets/"
  RSS_path = "/var/www/html/airead/assets/rss/"
  # RSS_path = "/var/rprojects/Projects/AireAD/RSS/"
  
  margin_mail = 1 #number of hours
  margin_old_data = 3 #number of hours
  
  hour_tweet = 8 #local time
  
  add_old_obs = FALSE
  # ---
  
  
  # FUNCTIONS ----
  
  source("/var/rprojects/RFunctions/calc_AQI.R")      # calculate AQ index
  source("/var/rprojects/RFunctions/post_tweet.R")    # post a Tweet
  source("/var/rprojects/RFunctions/send_email.R")    # send an email
  source("/var/rprojects/RFunctions/update_pollen.R") # fetch mails containing pollen data and create table
  
  # Function to translate languages to locales
  lang_to_locale <- function(df){
    df[df$lang=="CAT","lang"] <-"ca-ES"
    df[df$lang=="EN","lang"] <-"en-GB"
    df[df$lang=="FR","lang"] <-"fr-FR"
    return(df)
  }
  # Function to convert AQI band
  assign_AQI_bands <- function(df,aqi_name="aqi",info_file=Andorra_info_file,lang="CAT",add_colors=TRUE){
    
    file=read.xlsx(info_file,sheet = "web_index_colors_lang")
    
    if(!is.data.frame(file)){
      labels=c("Excel·lent","Bona","Regular","Deficient","Dolenta")
      colors=c()
    }else{
      labels <- file[,paste0("web_band_",lang)]
      colors <- file$web_color
    }
    
    df$band[df[aqi_name]>=1 & df[aqi_name]<2] <- labels[1]
    df$band[df[aqi_name]>=2 & df[aqi_name]<3] <- labels[2]
    df$band[df[aqi_name]>=3 & df[aqi_name]<4] <- labels[3]
    df$band[df[aqi_name]>=4 & df[aqi_name]<5] <- labels[4]
    df$band[df[aqi_name]>=5 & df[aqi_name]<6] <- labels[5]
    df$band[df[aqi_name]>=6]                  <- labels[6]
    
    
    if(add_colors){
      df$color <- file[match(df$band,file[,paste0("web_band_",lang)]),'web_color']
      # assign color for missing data
      df$color[is.na(df[aqi_name])] <- file[file$index==0,'web_color']
      names(df)[names(df)=="color"] <- paste0(aqi_name,"_color")
    }
    
    names(df)[names(df)=="band"] <- paste0(aqi_name,"_band_",lang)
    
    return(df)
  }
  # ---
  
  
  # Connect to the database ----
  con <- dbConnect(RPostgres::Postgres(),dbname=database,host=host,port=port,user=user,password=pwd)
  
  #get data from some of the tables
  # stations <- dbReadTable(con,"stations")
  
  
  #MAIN----
  #.Get data from hourly Andorra.txt file ----
  # load hourly data processed from ftp files
  # Andorra_hourly <- readRDS(AD_hourly_RDS)
  table5 <- readRDS(AD_hourly_RDS)
  
  
  # drop duplicated rows when there is a value for the same date
  # table5 <- Andorra_hourly %>% group_by(end_position) %>% 
  #                              group_by(sampling_point_id,.add=T) %>% 
  #                              slice(which.max(!is.na(value)))
  
  
  if(add_old_obs){
    # add old data from observations
    observing_capabilities <- dbReadTable(con,"observing_capabilities")
    
    df_obs <- dbReadTable(con, "observations") %>% select(-id)
    df_obs[df_obs$value==-999,"value"] <- NA
    df_obs[df_obs$import_value==-999,"import_value"] <- NA
    df_obs[df_obs$value==-9900,"value"] <- NA
    df_obs[df_obs$import_value==-9900,"import_value"] <- NA
    
  
    df <- df_obs %>% full_join(table5)
  }else{
    df <- table5
    
    rm(Andorra_hourly)
    
  }
  
  # keep validated data in case of duplication
  df <- df %>% 
    group_by(end_position) %>%
    group_by(sampling_point_id,.add=T) %>%
    # slice(which.min(!is.na(verification_flag)))
    dplyr::filter(verification_flag==min(verification_flag))
  
  df <- distinct(df)
  table5 <- df
  table5[is.na(table5$value),"value"] <- -999
  table5[is.na(table5$import_value),"import_value"] <- -999
  table5 <- table5 %>% dplyr::arrange(begin_position,sampling_point_id)
  
  dbWriteTable(con,"web_observations",table5,overwrite=TRUE)
  

  
  
  #.Process from web_observations ----
  list.SPO <- read_excel(Andorra_info_file,"web_RavenMapping")
  
  # df <- dbReadTable(con,"web_observations")
  
  
  df$station <- substr(df$sampling_point_id,5,11)
  df$pollutant <- substr(df$sampling_point_id,13,nchar(df$sampling_point_id))
  
  #filter meteo parameters
  meteo <- df %>% dplyr::filter(pollutant>=9000)
  df <- df %>% dplyr::filter(pollutant<9000)
  
  df$pollutant <- list.SPO[match(df$pollutant,list.SPO$pollutant_id),"notation"] %>% pull
  meteo$pollutant <- list.SPO[match(meteo$pollutant,list.SPO$pollutant_id),"notation"] %>% pull
  
  
  # for duplicated rows, keep the one with value (not NA)
  df_all <- df %>% group_by(end_position) %>%
                   group_by(sampling_point_id,.add=T) %>%
                   slice(which.max(!is.na(value)))
  meteo_all <- meteo %>% group_by(end_position) %>%
                         group_by(sampling_point_id,.add=T) %>%
                         slice(which.max(!is.na(value)))
  
  names(df_all)[names(df_all)=="end_position"] <- "date"  #date is end_time in UTC+1
  names(meteo_all)[names(meteo_all)=="end_position"] <- "date"  #date is end_time in UTC+1
  
  # df_all$date <- as.POSIXct(df_all$date,"%Y-%m-%dT%H:%M:%S+01:00",tz="Europe/Andorra")
  # meteo_all$date <- as.POSIXct(meteo_all$date,"%Y-%m-%dT%H:%M:%S+01:00",tz="Europe/Andorra")
  df_all$date <- as.POSIXct(df_all$date,"%Y-%m-%dT%H:%M:%S+01:00",tz="Etc/GMT-1")
  meteo_all$date <- as.POSIXct(meteo_all$date,"%Y-%m-%dT%H:%M:%S+01:00",tz="Etc/GMT-1")
  
  
  # filter concentration >-10
  df_all <- df_all %>% dplyr::filter(value>-10 | is.na(value))
  
  # convert dataframes to have pollutants as columns
  data_col <- df_all %>% pivot_wider(c(station,date),pollutant)
  meteo_col <- meteo_all %>% pivot_wider(c(station,date),pollutant)
  
  
  proc.time()-ptm #0.3s
  
  
  
  #.Aggregations ----
  # ...running mean 24h PM10 ----
  if("PM10" %in% colnames(data_col)){
    for(st in unique(data_col$station)){
      df_pm <- data_col %>% dplyr::filter(station==st) %>%
                            rollingMean(.,pollutant = "PM10", width = 24, new.name = "PM10_24h",
                                        align = "right", data.capture = 75)
      data_col[data_col$station == st,"PM10"] <- round(df_pm$PM10_24h)
    }
  }
  if("PM2.5" %in% colnames(data_col)){
    for(st in unique(data_col$station)){
      df_pm <- data_col %>% dplyr::filter(station==st) %>%
        rollingMean(.,pollutant = "PM2.5", width = 24, new.name = "PM2.5_24h",
                    align = "right", data.capture = 75)
      data_col[data_col$station == st,"PM2.5"] <- round(df_pm$PM2.5_24h)
    }
  }
  
  #...running mean 8h CO ----
  if("CO" %in% colnames(data_col)){
    for(st in unique(data_col$station)){
      df_pm <- data_col %>% dplyr::filter(station==st) %>%
                            rollingMean(.,pollutant = "CO", width = 8, new.name = "CO_8h",
                                        align = "right", data.capture = 75)
      data_col[data_col$station == st,"CO"] <- round(df_pm$CO_8h)
    }
  }
  
  
  
  
  #.Extract last values ----
  #filtering rows with all NAs (except station and date)
  # last_obs <- data_col %>% slice_max(date)
  last_obs <- data_col %>% group_by(station) %>%
                           dplyr::filter(if_any(c(-date), ~!is.na(.))) %>%
                           slice_max(date)
  
  
  #.Temperature inversion ----
  meteo$date <- as.POSIXct(meteo$end_position,"%Y-%m-%dT%H:%M:%S+01:00",tz="Etc/GMT-1")
  
  T942 <- meteo %>% dplyr::filter(sampling_point_id=="SPO-AD0942A-9054") %>% select(date,value)
  T944 <- meteo %>% dplyr::filter(sampling_point_id=="SPO-AD0944A-9054") %>% select(date,value)
  
  inversion <- full_join(T944,T942, by="date", suffix=c("944","942"))
  
  # if we have both T942 and T944 for the last hour, check if we have inversion, otherwise set FALSE
  inversion <- inversion %>% slice_max(date)
  if(!is.na(inversion$value942) & !is.na(inversion$value944)){
    inversion$inversion <- inversion$value942<inversion$value944
  }else{
    inversion$inversion <- FALSE
  }
  
  
  
  #.Calculate AQI ----
  #...For each pollutant ----
  index_poll <- calc_AQI_dec(data_col,unique(df_all$pollutant))
  
  
  #...Global AQI ----
  # global AQI is the max of all columns except date and station
  index_poll$aqi <- apply(index_poll[,-c(1,2)],1,max,na.rm=TRUE)

  # replace -inf for NAs
  index_poll[index_poll==-Inf] <- NA
  
  #...Find Culprit ----
  index_poll$culprit <- names(index_poll)[which(index_poll==index_poll$aqi,arr.ind=T)[,"col"]][1:nrow(index_poll)] %>%
                        substring(.,7,nchar(.))
  
  
  #...Convert AQIs to bands with labels ----
  for(name in names(index_poll)){
    if(grepl("index_",name) | name=="aqi"){
      index_poll <- assign_AQI_bands(index_poll, aqi_name=name, lang="CAT")
      index_poll <- assign_AQI_bands(index_poll, aqi_name=name, lang="EN",add_colors=F)
      index_poll <- assign_AQI_bands(index_poll, aqi_name=name, lang="FR",add_colors=F)
    }
  }
  
  
  
  # WEB TABLES ----
  #...0: Mapping Table----
  web_mapping <- read.xlsx(Andorra_info_file,"web_RavenMapping_lang")
  table0 <- web_mapping %>% select(pollutant_id,unite,notation,web_pollutant,web_units,web_units_graph,pollutant_name)
  
  # drop duplicated rows
  table0 <- table0 %>% distinct()
  
  
  #. . . . . colors table----
  table_0_colors <- read.xlsx(Andorra_info_file,"web_colors",colNames = F)
  names(table_0_colors) <- "colors"
  
  
  
  
  #...1: Latest Data----
  #read which pollutants to expect in each station
  pollutant_station <- read.xlsx(Andorra_info_file,"pollutants_stations") %>%
    pivot_longer(-station,"pollutant") %>%
    dplyr::filter(value==1) %>%
    select(-value)
  
  # date is end_time in UTC+1
  last_index <- index_poll %>% 
     group_by(station) %>% 
     slice_max(date) %>%
     full_join(last_obs) %>%
     select(-c(starts_with("aqi"),culprit)) %>%
     dplyr::filter(date==max(index_poll$date))
     # mutate_if(is.numeric, floor)  #indexes as lowest integers
  
  table1 <- last_index %>% 
    pivot_longer(.,cols = unique(df_all$pollutant),
                                names_to = "pollutant",
                                values_to = "concentration") %>%
    select(date,station,pollutant,concentration)
  
  # add index and bands as columns
  for(pol in unique(df_all$pollutant)){
    if(paste0("index_",pol) %in% names(last_index)){
      table1$index[table1$pollutant==pol] <- last_index[paste0("index_",pol)] %>% pull
      table1$band_CAT[table1$pollutant==pol] <- last_index[paste0("index_",pol,"_band_CAT")] %>% pull
      table1$band_EN[table1$pollutant==pol] <- last_index[paste0("index_",pol,"_band_EN")] %>% pull
      table1$band_FR[table1$pollutant==pol] <- last_index[paste0("index_",pol,"_band_FR")] %>% pull
      table1$color[table1$pollutant==pol] <- last_index[paste0("index_",pol,"_color")] %>% pull
    }
  }
  
  
  # get aggregation period in different languages
  table1$period_CAT <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_period_CAT']
  table1$period_EN <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_period_EN']
  table1$period_FR <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_period_FR']
  
  #filter out NOx and NO (pollutants with no index)
  table1 <- table1 %>% dplyr::filter(!pollutant %in% c("NOX","NO"))
  
  
  # order table according to index
  table1 <- table1 %>% dplyr::arrange(desc(index)) %>% mutate(index=floor(index))
  # get floor of concentration except for CO
  table1$concentration[table1$pollutant!="CO"] <- floor(table1$concentration[table1$pollutant!="CO"])
  
  # find units in html code for each pollutant
  units <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_units']
  
  # paste units into concentration column (character)
  table1$concentration <- paste(table1$concentration,units)
  
  #drop pollutants not measured in each station
  table1 <- table1 %>% right_join(pollutant_station,by=c("station","pollutant"))
  
  # convert pollutants to html code
  table1$pollutant <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_pollutant']
  
  # if NA, change for "-" or zero
  table1[c("band_CAT","band_EN","band_FR")][is.na(table1[c("band_CAT","band_EN","band_FR")])] <- "-"
  table1$concentration[is.na(table1$index)] <- "-"
  table1$index[is.na(table1$index)] <- "-"
  
  
  # change from UTC+1 to local time (taking into account winter/summer time)
  table1$date <- with_tz(ymd_hms(table1$date,tz="Etc/GMT-1"),"Europe/Andorra")
  
  
  
  # assign ID to each row
  table1 <- tibble::rowid_to_column(table1,"id")
  table1$id <- paste0("LD",table1$id)
  
  # break table in two to separate translations
  table1_common <- table1 %>% select(everything(),-(starts_with(c("band","period"))))
  table1_lang <- table1 %>% ungroup() %>% select(id,starts_with(c("band","period")))
  
  # transform bands and period to rows
  table1_lang <- table1_lang %>% pivot_longer(cols=starts_with("band"),
                                                    names_to="lang",
                                                    values_to="band") %>%
                                       pivot_longer(cols=starts_with("period"),
                                                    names_to="lang_p",
                                                    values_to="period")
  
  # extract only language from lang columns
  table1_lang$lang <- substr(table1_lang$lang,6,length(table1_lang$lang))
  table1_lang$lang_p <- substr(table1_lang$lang_p,8,length(table1_lang$lang_p))
  
  # select rows in which lang and lang_p match
  table1_lang <- table1_lang %>% 
    dplyr::filter(lang==lang_p) %>% 
    select(-lang_p)
  
  # change language name to locale
  table1_lang <- lang_to_locale(table1_lang)
  
  
  
  #...2: Station info ----
  table2 <- read.xlsx(Andorra_info_file,"web_stations_lang") %>%
            select(station_id,contains("web_"))
  
  table2$station_id <- substring(table2$station_id,5)
  
  
  # break table in two to separate translations
  table2_common <- table2 %>% select(everything(),-(ends_with(c("CAT","FR","EN"))))
  table2_lang <- table2 %>% ungroup() %>% select(station_id,ends_with(c("CAT","FR","EN")))
  
  # transform bands and period to rows
  table2_lang <- table2_lang %>% pivot_longer(cols=starts_with("web_type"),
                                        names_to="lang",
                                        values_to="type") %>%
                                  pivot_longer(cols=starts_with("web_ubicacio"),
                                        names_to="lang_u",
                                        values_to="location") %>%
                                  pivot_longer(cols=starts_with("web_comentaris"),
                                        names_to="lang_c",
                                        values_to="commments")
    
  # extract only language from lang columns
  table2_lang$lang <- substr(table2_lang$lang,10,nchar(table2_lang$lang))
  table2_lang$lang_u <- substr(table2_lang$lang_u,14,nchar(table2_lang$lang_u))
  table2_lang$lang_c <- substr(table2_lang$lang_c,16,nchar(table2_lang$lang_c))
  
  # select rows in which languages match
  table2_lang <- table2_lang %>% dplyr::filter(lang==lang_u & lang==lang_c) %>% 
                                 select(-c(lang_u,lang_c))
  
  # change language name to locale
  table2_lang <- lang_to_locale(table2_lang)
  
  
  
  #. . . . . Dades mapping----
  table_0_dades <- data.frame("timeserieid"=unique(table5$sampling_point_id))
  
  table_0_dades$station <- substr(table_0_dades$timeserieid,5,11)
  table_0_dades$pollutant <- substr(table_0_dades$timeserieid,13,nchar(table_0_dades$timeserieid))
  
  #do not include meteo
  table_0_dades <- table_0_dades %>% dplyr::filter(pollutant<9000)
  
  table_0_dades$pollutant_name <- table0[match(table_0_dades$pollutant,table0$pollutant_id),'notation']
  table_0_dades$station_name <- table2[match(table_0_dades$station,table2$station_id),'web_name_noaccents']
  
  table_0_dades$label <- paste0(table_0_dades$station_name,", ",table_0_dades$pollutant_name)
  
  
  #match returns the FIRST match
  table_0_dades$fromtime <- table5[match(table_0_dades$timeserieid,table5$sampling_point_id),'begin_position'] %>% pull
  
  # the same as the previous line but for the LAST match
  table_0_dades <- table_0_dades %>% dplyr::mutate(totime = pull(table5[max(which(timeserieid==table5$sampling_point_id)),'begin_position']))
  
  # drop columns station,pollutant,station_name,pollutant_name
  table_0_dades <- table_0_dades %>% select(-station,-station_name,-pollutant,-pollutant_name)
  
  
  #...3: Map: Global index per station ----
  #get only latest data of all available data (the map should show info of the same time for all stations)
  #TODO CHECK!!! this was making the map take the data from the previous hour
  # table3 <- index_poll %>% 
  #   dplyr::filter(date==format(max(index_poll$date),"%Y-%m-%d %H:%M:%S")) %>%
  #   select(date,station,starts_with(c("aqi","aqi_band")),aqi_color)
  
  table3 <- index_poll %>%
    dplyr::filter(date==max(index_poll$date)) %>%
    select(date,station,starts_with(c("aqi","aqi_band")),aqi_color)
  
  
  # change from UTC+1 to local time (taking into account winter/summer time)
  table3$date <- with_tz(ymd_hms(table3$date,tz="Etc/GMT-1"),"Europe/Andorra")
  
  
  #replace NAs with "-" or 0
  table3["aqi_color"][is.na(table3["aqi"])] <- "#696969"
  # table3$aqi <- as.character(table3$aqi)
  # table3["aqi"][is.na(table3["aqi"])] <- ""
  # table3["aqi"][is.na(table3["aqi"])] <- NA
  table3["aqi"][is.na(table3["aqi"])] <- 0
  table3[c("aqi_band_CAT","aqi_band_EN","aqi_band_FR")][is.na(table3[c("aqi_band_CAT","aqi_band_EN","aqi_band_FR")])] <- "-"
  
  
  table3$web_global <- table2[match(table3$station,table2$station_id),'web_global']
  table3$web_name <- table2[match(table3$station,table2$station_id),'web_name']
  
  # Calc X,Y: station position in % of map image dimensions
  # (0,0) is left upper corner, (100,100) bottom rigth corner
  # assuming image is cropped to country borders...
  lat0=42.655784
  lat100=42.428791
  lon0=1.408520
  lon100=1.787182
  
  lat <- table2[match(table3$station,table2$station_id),'web_lat']
  lon <- table2[match(table3$station,table2$station_id),'web_long']
  
  table3$x_value <- 100*(lon-lon0)/(lon100-lon0)
  table3$y_value <- 100*(lat0-lat)/(lat0-lat100)
  
  # change name of station column and move it to first column
  names(table3)[names(table3)=="station"] <- "station_id"
  table3 <- table3 %>% select(station_id,everything())
  

  
  # break table in two to separate translations
  table3_common <- table3 %>% select(everything(),-starts_with("aqi_band"))
  table3_lang <- table3 %>% select(station_id,starts_with("aqi_band"))
  
  # transform bands and period to rows
  table3_lang <- table3_lang %>% pivot_longer(cols=starts_with("aqi_band"),
                                              names_to="lang",
                                              values_to="aqi_band")
  
  # extract only language from lang column
  table3_lang$lang <- substr(table3_lang$lang,10,nchar(table3_lang$lang))
  
  # change language name to locale
  table3_lang <- lang_to_locale(table3_lang)
  
  

  
  
  #...5: Graph ----
  # filter last N days and not include meteo
  

  ptm <- proc.time()
  N_days = 30
  
  # table5_graph <- table5 %>% dplyr::mutate(end_position = as.POSIXct(end_position,"%Y-%m-%dT%H:00:00+01:00",tz="Etc/GMT-1"))
  # proc.time()-ptm
  
  N_days_ago <- now(tzone="Etc/GMT-1")-ddays(N_days)
  
  table5_graph <- table5 %>%
    dplyr::filter(to_time>=N_days_ago) %>%
    dplyr::filter(substr(sampling_point_id,13,nchar(sampling_point_id))<9000)
  
  # table5_graph <- table5_graph %>%
  #   dplyr::filter(end_position>=N_days_ago) %>%
  #   dplyr::filter(substr(sampling_point_id,13,nchar(sampling_point_id))<9000) %>%
  #   dplyr::mutate(end_position = format.Date(end_position,"%Y-%m-%dT%H:%M:%S+01:00"))
  proc.time()-ptm
  
  #FIXME from_time, to_time
  
  # change from UTC+1 to local time (taking into account winter/summer time)
  table5_graph$date_local <- with_tz(ymd_hms(table5_graph$end_position,tz="Etc/GMT-1"),"Europe/Andorra")
  

  proc.time()-ptm
  
  
  #...6: Utils ----
  lastUpdate <- max(last_obs$date)
  
  #convert to local time
  lastUpdate <- with_tz(ymd_hms(lastUpdate,tz="Etc/GMT-1"),"Europe/Andorra")
  
  
  # text to unify date and hour
  text_between_datetime_CAT <- "a les"
  text_between_datetime_EN <- "at"
  text_between_datetime_FR <- "&agrave;"
  
  
  #number of hours since last update
  count_last_update <- as.numeric(difftime(now(),lastUpdate,units="hours"))
  
  # should we show "No data" page?
  old_data_bool <- ifelse(count_last_update>=margin_old_data,TRUE,FALSE)
  
  # message to show after 3h with no new data
  OldDataMessage_CAT <- "Per problemes t&egrave;cnics no estem rebent dades en temps real"
  OldDataMessage_EN <- "Due to technical issues we are not receiving real time data"
  OldDataMessage_FR <- "En raison de probl&egrave;mes techniques, nous ne recevons pas de donn&eacute;es en temps r&eacute;el"
  
  #. . . . . mails----
  # previous_count <- dbReadTable(con,"web_utils") %>% select(count_h) %>% slice(1) %>% pull()
  

  # if((previous_count>margin_mail) & (previous_count<(margin_mail+1))){
  #   # we are not receiving new data
  #   # send email type_of_email="no_data"
  #   send_email("no_data")
  # }
  # if((count_last_update<previous_count) & (count_last_update>=1)){
  #   # we are receiving data again (and were not receiving data on the previous hour)
  #   # send email type_of_email="restablished"
  #   send_email("restablished")
  # }

  
  # Temperature inversion
  # read messages from Andorra_info
  inversion_messages <- read.xlsx(Andorra_info_file,'web_inversion')
  
  if(inversion$inversion==TRUE){
    inversion$message_CAT <- inversion_messages$true_html_CAT
    inversion$message_EN <- inversion_messages$true_html_EN
    inversion$message_FR <- inversion_messages$true_html_FR
    # inversion$color <- "#FFCAB0"
  }else{
    inversion$message_CAT <- inversion_messages$false_html_CAT
    inversion$message_EN <- inversion_messages$false_html_EN
    inversion$message_FR <- inversion_messages$false_html_FR
    # inversion$color <- "#FFFFFF"
  }
  
  
  table6 <- data.frame("id"=c("U1","U2","U3"),
                       "last_update"=lastUpdate,
                       "text_between_datetime"=c(text_between_datetime_CAT,
                                                 text_between_datetime_EN,
                                                 text_between_datetime_FR),
                       "old_data_message"=c(OldDataMessage_CAT,
                                          OldDataMessage_EN,
                                          OldDataMessage_FR),
                       "old_data_bool"=old_data_bool,
                       "inversion_message"=c(inversion$message_CAT,
                                     inversion$message_EN,
                                     inversion$message_FR),
                       "inversion_bool" = inversion$inversion,
                       "inversion_moreinfo_text" = c(inversion_messages$moreinfo_CAT,
                                                     inversion_messages$moreinfo_EN,
                                                     inversion_messages$moreinfo_FR),
                       "inversion_moreinfo_url" = inversion_messages$moreinfo_url,
                       "lang"=c("ca-ES","en-GB","fr-FR"),
                       "count_h"=count_last_update)
  
  
  #...7: web_daily_aq_poll ----
  # daily index of each pollutants (worst of hourly indexes / 24h for PM)
  # + station worst index
  # + global worst of all stations
  

  daily_maxs <- index_poll %>% group_by(day=as.Date(date)) %>% 
                               group_by(station,.add=TRUE) %>% 
                               # dplyr::summarise(across(index_SO2:aqi,max,na.rm=T))
                               # dplyr::summarise(across(index_O3:aqi,max,na.rm=T))
                               dplyr::summarise(across(c(index_O3,index_SO2,index_NO2,index_CO,index_PM2.5,index_PM10,aqi),max,na.rm=T))
  
  daily_maxs <- daily_maxs %>% pivot_longer(cols=starts_with("index"),
                                             names_to="pollutant",
                                             values_to="index")
  
  #change pollutant name to only include pollutant
  daily_maxs$pollutant <- substr(daily_maxs$pollutant,7,nchar(daily_maxs$pollutant))
  
  #replace -Inf with NA
  daily_maxs[daily_maxs==-Inf] <- NA
  
  # transform indexes to bands
  for(name in c("index","aqi")){
    daily_maxs <- assign_AQI_bands(daily_maxs, aqi_name=name, lang="CAT")
    daily_maxs <- assign_AQI_bands(daily_maxs, aqi_name=name, lang="EN", add_colors=F)
    daily_maxs <- assign_AQI_bands(daily_maxs, aqi_name=name, lang="FR", add_colors=F)
  }
  
  
  table7 <- daily_maxs %>% select(station,day,pollutant,starts_with(c("index_band","aqi")))
  
  # assign ID to each row
  table7 <- tibble::rowid_to_column(table7,"id")
  table7$id <- paste0("pAQ",table7$id)
  
  #order calendar table by specific order in stations and pollutants
  order_st <- c("AD0942A","AD0944A","AD0945A","AD0940A","AD0941A","AD0943A")
  order_pol <- c("SO2","CO","NO2","O3","PM10","PM2.5")
  table7 <- table7[order(factor(table7$station, levels = order_st)),]
  table7 <- table7[order(factor(table7$pollutant, levels = order_pol)),]
  
  
  # break table in two to separate translations
  table7_common <- table7 %>% select(everything(),-aqi,-ends_with(c("CAT","EN","FR")))
  table7_lang <- table7 %>% ungroup() %>% select(id,ends_with(c("CAT","EN","FR")))
  
  # transform bands and period to rows
  table7_lang <- table7_lang %>% pivot_longer(cols=starts_with("index_band"),
                                              names_to="lang",
                                              values_to="pollutant_band") %>%
                                 pivot_longer(cols=starts_with("aqi_band"),
                                              names_to="lang_a",
                                              values_to="aqi_band")
  
  # extract only language from lang columns
  table7_lang$lang <- substr(table7_lang$lang,12,nchar(table7_lang$lang))
  table7_lang$lang_a <- substr(table7_lang$lang_a,10,nchar(table7_lang$lang_a))
  
  # select rows in which languages match
  table7_lang <- table7_lang %>% dplyr::filter(lang==lang_a) %>% select(-lang_a)
  
  # change language name to locale
  table7_lang <- lang_to_locale(table7_lang)
  
  
  
  #...8: web_daily_aq_global----
  # global worst of day
  # table8 <- daily_maxs %>% group_by(day) %>% dplyr::summarise(country_aqi = max(aqi,na.rm = T))
  
  # only from stations with web_global=TRUE
  table8 <- daily_maxs %>%
    group_by(day) %>%
    dplyr::filter(station %in% table3$station_id[table3$web_global==TRUE]) %>%
    dplyr::summarise(country_aqi = max(aqi,na.rm=T))
  
  #replace -Inf with NA
  table8[table8==-Inf] <- NA
  
  #assign bands
  table8 <- assign_AQI_bands(table8,aqi_name="country_aqi",lang="CAT")
  table8 <- assign_AQI_bands(table8,aqi_name="country_aqi",lang="EN",add_colors=F)
  table8 <- assign_AQI_bands(table8,aqi_name="country_aqi",lang="FR",add_colors=F)
  
  # assign ID to each row
  table8 <- tibble::rowid_to_column(table8,"id")
  table8$id <- paste0("GAQ",table8$id)
  
  # break table in two to separate translations
  table8_common <- table8 %>% select(id,day,country_aqi,country_aqi_color)
  table8_common$country_aqi <- floor(table8_common$country_aqi) #keep only first number of AQI
  
  table8_lang <- table8 %>% ungroup() %>% select(id,ends_with(c("CAT","EN","FR")))
  
  table8_lang <- table8_lang %>% pivot_longer(cols=starts_with("country"),
                                              names_to="lang",
                                              values_to="band")
  
  # extract only language from lang columns
  table8_lang$lang <- substr(table8_lang$lang,18,nchar(table8_lang$lang))
  
  # change language name to locale
  table8_lang <- lang_to_locale(table8_lang)
  
  
  
  
  #UPLOAD Tables to Raven ----
  proc.time()-ptm #17s
  
  dbWriteTable(con,"web_mapping",table0,overwrite=TRUE)
  dbWriteTable(con,"web_graphic_colors",table_0_colors,overwrite=TRUE)
  dbWriteTable(con,"web_mapping_data",table_0_dades,overwrite=TRUE)
  
  dbWriteTable(con,"web_latest_data",table1_common,overwrite=TRUE)
  dbWriteTable(con,"web_latest_data_lang",table1_lang,overwrite=TRUE)
  
  dbWriteTable(con,"web_stations",table2_common,overwrite=TRUE)
  dbWriteTable(con,"web_stations_lang",table2_lang,overwrite=TRUE)
  
  dbWriteTable(con,"web_map",table3_common,overwrite=TRUE)
  dbWriteTable(con,"web_map_lang",table3_lang,overwrite=TRUE)
  
  dbWriteTable(con,"web_observations_graph",table5_graph,overwrite=TRUE)
  
  dbWriteTable(con,"web_utils",table6,overwrite=TRUE)
  
  dbWriteTable(con,"web_daily_aq_poll",table7_common,overwrite=TRUE)
  dbWriteTable(con,"web_daily_aq_poll_lang",table7_lang,overwrite=TRUE)
  
  dbWriteTable(con,"web_daily_aq_global",table8_common,overwrite=TRUE)
  dbWriteTable(con,"web_daily_aq_global_lang",table8_lang,overwrite=TRUE)
  
  proc.time()-ptm #18s
  
  
  
  # UPDATE OBSERVATIONS ----
  #...With POST----
  #FIXME: HTTP/1.1 400 BAD REQUEST
  # POST("http://116.203.249.237:8888/imports/observations",authenticate("admin","admin"),
  #      body=list(csv=upload_file("/var/rprojects/DATA/Andorra_hourly_GS.csv")),
  #      verbose())
  # system("curl -X POST -u admin:admin --form csv=@/var/rpojects/DATA/Andorra_hourly_GS.csv http://116.203.249.237:8888/imports/observations")
  
  
  #...With DBI----
  # filter stations in observing_capabilities
  # Andorra_to_observations <- table5 %>% dplyr::filter(sampling_point_id %in% observing_capabilities$sampling_point_id)
  # 
  # df_obs <- dbReadTable(con, "observations") %>% select(-id)
  # 
  # #difference between Andorra_to_observations and df_all: append only rows not found in df_all
  # # Andorra_append <- Andorra_to_observations %>% anti_join(df_obs)
  # Andorra_append <- anti_join(Andorra_to_observations,df_obs, by=c("sampling_point_id",
  #                                                                  "begin_position","end_position",
  #                                                                  "value","verification_flag",
  #                                                                  "validation_flag"))
  # 
  # #assign id numer to each row starting from last id in observations table
  # # Andorra_append$id <- seq(max(df_obs$id)+1,max(df_obs$id)+nrow(Andorra_append))
  # 
  # new_observations <- full_join(df_obs,Andorra_append)
  # 
  # #change NAs to -999
  # new_observations[is.na(new_observations$value),"value"] <- -999
  # new_observations[is.na(new_observations$import_value),"import_value"] <- -999
  # # #change NAs to -9900
  # new_observations[is.na(new_observations$value),"value"] <- -9900
  # new_observations[is.na(new_observations$import_value),"import_value"] <- -9900
  
  # 
  # new_observations$validation_flag <- as.integer(new_observations$validation_flag)
  # new_observations$verification_flag <- as.integer(new_observations$verification_flag)
  # 
  # 
  # # keep validated data in case of duplication
  # new_observations <- new_observations %>% group_by(end_position) %>% 
  #                                         group_by(sampling_point_id,.add=T) %>% 
  #                                         slice(which.min(!is.na(verification_flag)))
  # 
  # new_observations <- distinct(new_observations)
  # 
  # #assign id column
  # new_observations <- tibble::rowid_to_column(new_observations,"id")
  # 
  # 
  # # append new rows to observations table in Raven
  # #this takes around 7 seconds...
  # # dbWriteTable(con,"observations",new_observations,overwrite=TRUE)
  # dbWriteTable(con,"observations",new_observations,overwrite=TRUE,field.types=c(value="numeric(255,5)",import_value="numeric(255,5)"))
  # 
  

  
  
  # TWEET ----
  # AQI
  
  # get worst index band filtering from table 3 only those with web_global=TRUE
  countryAQ <- table3 %>% group_by(web_global) %>%
    slice(which.max(aqi)) %>%
    dplyr::filter(web_global==TRUE) %>%
    ungroup()
  
  #undo html format
  if(countryAQ$aqi_band_CAT=="Excel&middot;lent"){
    countryAQ$aqi_band_CAT=="Excel·lent"
  }
  
  if(hour(now())==hour_tweet){
    
    tweet <- paste0("La qualitat de l'#aire a #Andorra a les ",hour(now()),":00 és ",
                     countryAQ$aqi_band_CAT," - índex ",floor(countryAQ$aqi),
                     "/6. (+info aire.ad) #qualitataire")
    
    cat(tweet,file=paste0(tweets_path,"tweet_AQI.txt"))
    
    if(FALSE){
      # post_tweet(tweet)
    }
    
  }
  
  
  
  # RSS ----
  source("/var/rprojects/RScripts/write_xml.R", local = T)
  
  
  # MAIL ALIVE----
  # at 8h and 16h (Mon-Fri) send mail to know that this is alive
  if(hour(now()) %in% c(8,16)){
    if(!wday(now(),week_start = 1) %in% c(6,7)){
      send_email("alive",
                 paste0("La qualitat de l'aire a Andorra a les ",hour(now()),":00 és ",
                        countryAQ$aqi_band_CAT," - índex ",floor(countryAQ$aqi),"/6."))
    }
  }
  
  
  # POLLEN ----
  # if(FALSE){
    # update_pollen(con)
  # }
  update_pollen(con)
  
  

  # list all tables in the database
  # dbListTables(con)
  
  #Close database connection ----
  dbDisconnect(con)
  
  
  proc.time()-ptm 
  
}


# RUN the update function ----
print(Sys.time())
t <- try(update_raven())
print(Sys.time())

# if the update had errors, run the following
if("try-error" %in% class(t)){

  # change table_out$error_script
  table_out <- read.csv("/var/rprojects/Projects/AireAD/table_errors.csv")
  table_out[nrow(table_out),"error_script"] <- TRUE

  #overwrite table
  write.table(table_out, file = "/var/rprojects/Projects/AireAD/table_errors.csv", sep=",", row.names=F)

  # send mail letting us know that the script failed
  send_email("fail")

  #TODO append error to email

}
