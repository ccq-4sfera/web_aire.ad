# Connect to RAVEN database to download observations, calculate AQIndex etc
# Author: Cristina Carnerero
# Date: 16/12/2021

ptm <- proc.time() #start counting time

library(RPostgres)
library(DBI)
library(tidyr)
library(openxlsx)
library(plyr)
library(openair)
library(httr)

# PARAMETERS ----
# DB Raven
database = "ravendb"
host = "localhost"
port = 5432
user = "ravendb"
pwd = "ravendb"

# Others
from_table="observations"
# from_table="web_observations"

Andorra_info_file = "/var/rprojects/Projects/AireAD/Andorra_info.xlsx"
AD_hourly_RDS = "/var/rprojects/DATA/Andorra_hourly.RDS"
AD_hourly_csv = "/var/rprojects/DATA/Andorra_hourly.csv"
# ---


# FUNCTIONS ----
source("/var/rprojects/RFunctions/calc_AQI.R")
# ---

# Connect to the database ----
con <- dbConnect(RPostgres::Postgres(),dbname=database,host=host,port=port,user=user,password=pwd)

# list all tables in the database
# dbListTables(con)

#get data from one of the tables
stations <- dbReadTable(con,"stations")
list.SPO <- dbReadTable(con,"observing_capabilities")
eea_poll <- dbReadTable(con,"eea_pollutants")


#MAIN----
#.Get data for each SPO----

# translate pollutant vocabulary to pollutant name
list.SPO$pollutant <- eea_poll[match(list.SPO$pollutant,eea_poll$uri),'notation']
list.SPO$units <- strsplit(list.SPO$concentration[1],"/") %>% 
                  dplyr::last() %>% dplyr::last()

#FIXME only valid for Andorra 
#TODO find equivalency SPO-station in the tables in the database
list.SPO$station <- substr(list.SPO$sampling_point_id,5,11)


#---
#FIXME
#read from web_observations
# list.SPO2 <- read_excel(Andorra_info_file,"web_RavenMapping")
# 
# df <- dbReadTable(con,"web_observations")
# 
# df$station <- substr(df$sampling_point_id,5,11)
# df$pollutant <- substr(df$sampling_point_id,13,nchar(df$sampling_point_id))
# 
# #FIXME FILTER OUT METEO
# df <- df %>% dplyr::filter(pollutant<9000)
# 
# df$pollutant <- list.SPO2[match(df$pollutant,list.SPO2$pollutantID),"notation"]
# 
# df_all_raw <- df %>% select(-station,-pollutant)
# df_all <- df
# 
# # for duplicated rows, keep the one with value (not NA)
# df_all <- df_all %>% group_by(end_position) %>% 
#   group_by(sampling_point_id,.add=T) %>% 
#   slice(which.max(!is.na(value)))
# 
# names(df_all)[names(df_all)=="end_position"] <- "date"  #date is end_time in UTC+1
# df_all$date <- as.POSIXct(df_all$date,"%Y-%m-%dT%H:%M:%S+01:00",tz="Europe/Andorra")
# 
# 
# data_col2 <- df_all %>% pivot_wider(c(station,date),pollutant)

#---


for(i in 1:nrow(list.SPO)){
  SPO <- list.SPO$sampling_point_id[i]
  # get data
  df <- dbGetQuery(con, paste0("SELECT * FROM ",from_table," WHERE sampling_point_id = '",SPO,"'"))
  # all datetimes are in UTC+1

  # df <- dbReadTable(con,"web_observations")
  
  if(nrow(df)>0){
    # replace -999 with NAs
    df <- df %>% na_if(.,-999)
    
    # df <- df %>% select(sampling_point_id:validation_flag)
    
    df$station <- list.SPO$station[i]
    df$pollutant <- list.SPO$pollutant[i]
    df$units <- list.SPO$units[i]
    
    if(exists("df_all") && is.data.frame(get("df_all"))){
      df_all <- rbind.fill(df_all, df)
    }else{
      df_all <- df
    }
  }else{
    print(paste0("No data found for ",SPO))
  }
}

df_all_raw <- df_all %>% select(-station,-pollutant,-units)

# for duplicated rows, keep the one with value (not NA)
df_all <- df_all %>% group_by(end_position) %>% 
                      group_by(sampling_point_id,.add=T) %>% 
                      slice(which.max(!is.na(value)))

names(df_all)[names(df_all)=="end_position"] <- "date"  #date is end_time in UTC+1
df_all$date <- as.POSIXct(df_all$date,"%Y-%m-%dT%H:%M:%S+01:00",tz="UTC+01")

# df_all$date <- as.POSIXct(df_all$date,"%Y-%m-%dT%H:%M:%S+01:00",tz="Europe/Andorra") %>% 
#   format("%Y-%m-%d %H:%M")

data_col <- df_all %>% pivot_wider(c(station,date),pollutant)


proc.time()-ptm #5s



#.Aggregations ----
# ...running mean 24h PM10 ----
if("PM10" %in% colnames(data_col)){
  for(st in unique(data_col$station)){
    df_pm <- data_col %>% filter(station==st) %>%
                          rollingMean(.,pollutant = "PM10", width = 24, new.name = "PM10_24h",
                                      align = "right", data.capture = 75)
    data_col[data_col$station == st,"PM10"] <- round(df_pm$PM10_24h)
  }
}
if("PM2.5" %in% colnames(data_col)){
  for(st in unique(data_col$station)){
    df_pm <- data_col %>% filter(station==st) %>%
      rollingMean(.,pollutant = "PM2.5", width = 24, new.name = "PM2.5_24h",
                  align = "right", data.capture = 75)
    data_col[data_col$station == st,"PM2.5"] <- round(df_pm$PM2.5_24h)
  }
}

#...running mean 8h CO ----
if("CO" %in% colnames(data_col)){
  for(st in unique(data_col$station)){
    df_pm <- data_col %>% filter(station==st) %>%
                          rollingMean(.,pollutant = "CO", width = 8, new.name = "CO_8h",
                                      align = "right", data.capture = 75)
    data_col[data_col$station == st,"CO"] <- round(df_pm$CO_8h)
  }
}


#.Extract last values
#filtering rows with all NAs (except station and date)
# last_obs <- data_col %>% slice_max(date)
last_obs <- data_col %>% group_by(station) %>%
                         filter(if_any(c(-date), ~!is.na(.))) %>% 
                         slice_max(date)


#.Calculate AQI ----
#...For each pollutant ----
index_poll <- calc_AQI_dec(data_col,unique(df_all$pollutant))

#...Global AQI ----
# global AQI is the max of all columns except date and station
index_poll$aqi <- apply(index_poll[,-c(1,2)],1,max,na.rm=TRUE)
# remove infinites (when all indexes are NA)
index_poll <- index_poll %>% filter_at(vars(aqi), all_vars(is.finite(.)))

#...Find Culprit ----
index_poll$culprit <- names(index_poll)[which(index_poll==index_poll$aqi,arr.ind=T)[,"col"]][1:nrow(index_poll)] %>%
                      substring(.,7,nchar(.))


#...Convert AQIs to bands with labels ----
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
  df$band[df[aqi_name]>=5]                  <- labels[5]
  
  
  if(add_colors){
    df$color <- file[match(df$band,file[,paste0("web_band_",lang)]),'web_color']
    names(df)[names(df)=="color"] <- paste0(aqi_name,"_color")
  }
  
    names(df)[names(df)=="band"] <- paste0(aqi_name,"_band_",lang)

  return(df)
}

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
table0 <- web_mapping %>% select(pollutant_id,unite,notation,web_pollutant,web_units,pollutant_name)

#...1: Last observations----
# date is end_time in UTC+1
last_index <- index_poll %>% group_by(station) %>% 
                             slice_max(date) %>%
                             full_join(last_obs) %>%
                             select(-c(starts_with("aqi"),culprit)) %>%
                             mutate_if(is.numeric, floor)  #indexes as lowest integers

table1 <- last_index %>% pivot_longer(.,cols = unique(df_all$pollutant),
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

# find units in html code for each pollutant
units <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_units']

# get aggregation period in different languages
table1$period_CAT <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_period_CAT']
table1$period_EN <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_period_EN']
table1$period_FR <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_period_FR']

# convert pollutants to html code
table1$pollutant <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_pollutant']

# paste units into concentration column (character)
table1$concentration <- paste(table1$concentration,units)

# drop rows with NAs
table1 <- na.omit(table1)

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
table1_lang <- table1_lang %>% dplyr::filter(lang==lang_p) %>% 
                               select(-lang_p)


#...2: Station info ----
#FIXME falten coords de 941 i 945

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



#...3: Global index per station ----
table3 <- index_poll %>% group_by(station) %>%
                         slice_max(date) %>%
                         select(date,station,starts_with(c("aqi","aqi_band")),aqi_color)
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




#...5: web_observations ----
#TODO incorporar les dades de les altres estacions que no son al Raven

# load hourly data processed from ftp files
Andorra_hourly <- readRDS(AD_hourly_RDS)

# filter last N days
N_days = 30
table5 <- Andorra_hourly %>% 
            filter(as.POSIXct(end_position,"%Y-%m-%dT%H:00:00+01:00",tz="Europe/Andorra")>=(now(tz="Europe/Andorra")-ddays(N_days)))

# drop duplicated rows when there is a value for the same date
table5 <- table5 %>% group_by(end_position) %>% 
                     group_by(sampling_point_id,.add=T) %>% 
                     slice(which.max(!is.na(value)))



#...6: Utils ----
lastUpdate <- max(last_obs$date)
margin = 3
OldDataMessage_CAT <- paste0("Les dades es van actualitzar per &uacute;ltima vegada el ",
                             format(lastUpdate,"%d-%m-%Y a les %H:%M"))
OldDataMessage_EN <- paste0("The data were last updated on ",
                            format(lastUpdate,"%d-%m-%Y at %H:%M"))
OldDataMessage_FR <- paste0("Les donn&eacute;es ont &eacute;t&eacute; mises &agrave; jour pour la derni&egrave;re fois le ",
                            format(lastUpdate,"%d-%m-%Y &agrave; %H:%M"))

table6 <- data.frame("id"=c("U1","U2","U3"),
                     "lastUpdate"=lastUpdate,
                     "margin"=margin,
                     "OldDataMessage"=c(OldDataMessage_CAT,
                                        OldDataMessage_EN,
                                        OldDataMessage_FR),
                     "lang"=c("CAT","EN","FR"))



#...7: web_daily_AQ_poll ----
# daily index of each pollutants (worst of hourly indexes / 24h for PM)
# + station worst index
# + global worst of all stations
daily_maxs <- index_poll %>% group_by(day=as.Date(date)) %>% 
                             group_by(station,.add=TRUE) %>% 
                             dplyr::summarise(across(index_SO2:aqi,max))

daily_maxs <- daily_maxs %>% pivot_longer(cols=starts_with("index"),
                                           names_to="pollutant",
                                           values_to="index")

#change pollutant name to only include pollutant
daily_maxs$pollutant <- substr(daily_maxs$pollutant,7,nchar(daily_maxs$pollutant))

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

#TODO distingir no mesurat vs no hi ha dades



#...8: web_daily_AQ_global----
# global worst of day
table8 <- daily_maxs %>% group_by(day) %>% dplyr::summarise(country_aqi = max(aqi))
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





#UPLOAD Tables to Raven ----
proc.time()-ptm #54s

dbWriteTable(con,"web_mapping",table0,overwrite=TRUE)

dbWriteTable(con,"web_latest_data",table1_common,overwrite=TRUE)
dbWriteTable(con,"web_latest_data_lang",table1_lang,overwrite=TRUE)

dbWriteTable(con,"web_stations",table2_common,overwrite=TRUE)
dbWriteTable(con,"web_stations_lang",table2_lang,overwrite=TRUE)

dbWriteTable(con,"web_map",table3_common,overwrite=TRUE)
dbWriteTable(con,"web_map_lang",table3_lang,overwrite=TRUE)

#TODO with POST?
dbWriteTable(con,"web_observations",table5,overwrite=TRUE)

dbWriteTable(con,"web_utils",table6,overwrite=TRUE)

dbWriteTable(con,"web_daily_AQ_poll",table7_common,overwrite=TRUE)
dbWriteTable(con,"web_daily_AQ_poll_lang",table7_lang,overwrite=TRUE)

dbWriteTable(con,"web_daily_AQ_global",table8_common,overwrite=TRUE)
dbWriteTable(con,"web_daily_AQ_global_lang",table8_lang,overwrite=TRUE)



# UPDATE OBSERVATIONS ----
#...With POST----
#FIXME: HTTP/1.1 400 BAD REQUEST
# POST("http://116.203.249.237:8888/imports/observations",authenticate("admin","admin"),
#      body=list(csv=upload_file("/var/rprojects/DATA/Andorra_hourly_GS.csv")),
#      verbose())
# system("curl -X POST -u admin:admin --form csv=@/var/rpojects/DATA/Andorra_hourly_GS.csv http://116.203.249.237:8888/imports/observations")


#...With DBI----
# filter stations in observing_capabilities
Andorra_to_observations <- table5 %>% dplyr::filter(sampling_point_id %in% list.SPO$sampling_point_id)

# add missing columns (observations has more columns than Andorra_to_observations)
# Andorra_to_observations$touched <- now(tz="Europe/Andorra")
# Andorra_to_observations$from_time <- as.POSIXct(Andorra_to_observations$begin_position,"%Y-%m-%dT%H:%M:%S+01:00",tz="UTC+01")
# Andorra_to_observations$to_time <- as.POSIXct(Andorra_to_observations$end_position,"%Y-%m-%dT%H:%M:%S+01:00",tz="UTC+01")
# Andorra_to_observations$import_value <- Andorra_to_observations$value

#difference between Andorra_to_observations and df_all: append only rows not found in df_all
Andorra_append <- Andorra_to_observations %>% anti_join(df_all_raw)
#assign id numer to each row starting from last id in observations table
Andorra_append$id <- seq(max(df_all_raw$id)+1,max(df_all_raw$id)+nrow(Andorra_append))

new_observations <- full_join(df_all_raw,Andorra_append)

#change NAs to -999
new_observations[is.na(new_observations$value),"value"] <- -999
new_observations[is.na(new_observations$import_value),"import_value"] <- -999


# append new rows to observations table in Raven
#this takes around 7 seconds...
dbWriteTable(con,"observations",new_observations,overwrite=TRUE)




# list all tables in the database
dbListTables(con)

#Close database connection ----
dbDisconnect(con)


proc.time()-ptm #68s






# (insert meteo parameters into eea_pollutants table)----
if(FALSE){
  meteo_to_insert <- web_mapping %>% dplyr::filter(pollutant_id>9000) %>%
                                          select(pollutant_id,notation,label,eea_pollutant) %>%
                                          distinct()
  names(meteo_to_insert) <- c("id","notation","label","uri")
  
  #TODO update eea_pollutants table, check code...
  dbWriteTable(con,"eea_pollutants",value=meteo_to_insert,append=TRUE,overwrite=FALSE)
}
