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
info_file = "/var/rprojects/Projects/AireAD/Andorra_info.xlsx"
AD_hourly_RDS <- "/var/rprojects/DATA/Andorra_hourly.RDS"
AD_hourly_csv <- "/var/rprojects/DATA/Andorra_hourly.csv"
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

from_observations=TRUE
#FIXME:
# from_observations=FALSE
for(i in 1:nrow(list.SPO)){
  SPO <- list.SPO$sampling_point_id[i]
  # get data
  if(from_observations){
    df <- dbGetQuery(con, paste0("SELECT * FROM observations WHERE sampling_point_id = '",SPO,"'"))
   # all datetimes are in UTC+1
  }else{
    df <- readRDS(AD_hourly_RDS) %>% dplyr::filter(sampling_point_id==SPO)
  }
  
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
assign_AQI_bands <- function(df,aqi_name="aqi",file=read.xlsx(info_file,"web_index_colors")){
  
  if(!is.data.frame(file)){
    labels=c("Excel·lent","Bona","Regular","Deficient","Dolenta")
    colors=c()
  }else{
    labels <- file$web_band
    colors <- file$web_color
  }
  
  df$band[df[aqi_name]>=1 & df[aqi_name]<2] <- labels[1]
  df$band[df[aqi_name]>=2 & df[aqi_name]<3] <- labels[2]
  df$band[df[aqi_name]>=3 & df[aqi_name]<4] <- labels[3]
  df$band[df[aqi_name]>=4 & df[aqi_name]<5] <- labels[4]
  df$band[df[aqi_name]>=5]                  <- labels[5]
  
  df$color <- file[match(df$band,file$web_band),'web_color']
  
  names(df)[names(df)=="band"] <- paste0(aqi_name,"_band")
  names(df)[names(df)=="color"] <- paste0(aqi_name,"_color")
  return(df)
}

for(name in names(index_poll)){
  if(grepl("index_",name) | name=="aqi"){
    index_poll <- assign_AQI_bands(index_poll, aqi_name = name)
  }
}



# WEB TABLES ----
#...0: Mapping Table----
web_mapping <- read.xlsx(info_file,"web_RavenMapping")


#...1: Last observations----
# date is end_time in UTC+1
last_index <- index_poll %>% group_by(station) %>% 
                             slice_max(date) %>%
                             full_join(last_obs) %>%
                             select(-c(aqi_band,aqi,culprit)) %>%
                             mutate_if(is.numeric, floor)  #indexes as lowest integers

table1 <- last_index %>% pivot_longer(.,cols = unique(df_all$pollutant),
                                    names_to = "pollutant",
                                    values_to = "concentration") %>%
                         select(date,station,pollutant,concentration)

# add index and bands as columns
for(pol in unique(df_all$pollutant)){
  # table1$units[table1$pollutant==pol] <- list.SPO$units[list.SPO$pollutant==pol]
  table1$index[table1$pollutant==pol] <- last_index[paste0("index_",pol)] %>% pull
  table1$band[table1$pollutant==pol] <- last_index[paste0("index_",pol,"_band")] %>% pull
  table1$color[table1$pollutant==pol] <- last_index[paste0("index_",pol,"_color")] %>% pull
}

# find units and period in html code for each pollutant
units <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_units']
table1$period <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_period']

# convert pollutants to html code
table1$pollutant <- web_mapping[match(table1$pollutant,web_mapping$notation),'web_pollutant']

# paste units into concentration column (character)
table1$concentration <- paste(table1$concentration,units)

# drop rows with NAs
table1 <- na.omit(table1)


#...2: Station info ----
table2 <- read.xlsx(info_file,"web_stations") %>%
           select(station_id,contains("web_"))
table2$station_id <- substring(table2$station_id,5)
#FIXME falten coords de 941 i 945


#...3: Global index per station ----
table3 <- index_poll %>% group_by(station) %>% 
                         slice_max(date) %>% 
                         select(date,station,aqi,aqi_band,aqi_color)
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


#...4: Country global index ----
#get worst index band filtering only those with web_global=TRUE
# table4 <- table3 %>% dplyr::filter(web_global=TRUE) %>% 
#                      slice(which.max(table3$aqi)) %>%
#                      select(date,aqi_band,aqi_color)


#...5: web_observations ----
#TODO incorporar les dades de les altres estacions que no son al Raven
# table5 <- df_all %>% select(sampling_point_id,begin_position,end_position,
#                             value,verification_flag,validation_flag)

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
OldDataMessage <- paste0("Les dades es van actualitzar per &uacute;ltima vegada el ",
                         format(lastUpdate,"%d-%m-%Y a les %H:%M"))
table6 <- data.frame("lastUpdate"=lastUpdate,"margin"=margin,"OldDataMessage"=OldDataMessage)



#...7: Web_daily_AQ ----
# daily index of each pollutants (worst of hourly indexes / 24h for PM)
# + station worst index
# + global worst of all stations
daily_maxs <- index_poll %>% group_by(day=as.Date(date)) %>% 
                             group_by(station,.add=TRUE) %>% 
                             dplyr::summarise(across(index_SO2:aqi,max))

# different for last day (today): last value instead of worst of day
# daily_today <- index_poll %>% select(date,station,index_O3:aqi) %>%
#                               slice_max(date) %>%
#                               mutate(day=as.Date(date)) %>%
#                               select(day,everything(),-date)
# 
# daily_maxs[daily_maxs$day==unique(daily_today$day),] <- daily_today


# transform indexes to bands
for(name in names(daily_maxs)){
  if(grepl("index_",name) | name=="aqi"){
    daily_maxs <- assign_AQI_bands(daily_maxs, aqi_name = name)
  }
}

# global worst of day
daily_country <- daily_maxs %>% group_by(day) %>% dplyr::summarise(country_aqi = max(aqi))
daily_country <- assign_AQI_bands(daily_country,aqi_name = "country_aqi")

table7 <- daily_maxs %>% left_join(daily_country) %>% 
                         select(day,station,ends_with("band"),ends_with("aqi_color"))




#UPLOAD Tables to Raven ----
#TODO with POST?
proc.time()-ptm #11s

dbWriteTable(con,"web_mapping",web_mapping,row.names=TRUE,overwrite=TRUE)

dbWriteTable(con,"web_latest_data",table1,row.names=TRUE,overwrite=TRUE)
dbWriteTable(con,"web_stations",table2,row.names=TRUE,overwrite=TRUE)
dbWriteTable(con,"web_map",table3,row.names=TRUE,overwrite=TRUE)
# dbWriteTable(con,"web_aqi_global",table4,overwrite=TRUE)
dbWriteTable(con,"web_observations",row.names=TRUE,table5,overwrite=TRUE)
dbWriteTable(con,"web_utils",row.names=TRUE,table6,overwrite=TRUE)
dbWriteTable(con,"web_daily_AQ",row.names=TRUE,table7,overwrite=TRUE)




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
#+filter out future values
Andorra_append <- Andorra_to_observations %>% anti_join(df_all_raw) %>%
                                              dplyr::filter(to_time<touched)

#assign id numer to each row starting from last id in observations table
Andorra_append$id <- seq(max(df_all_raw$id)+1,max(df_all_raw$id)+nrow(Andorra_append))

new_observations <- full_join(df_all_raw,Andorra_append)
new_observations[is.na(new_observations)] <- -999


# append new rows to observations table in Raven
#this takes around 7 seconds...
dbWriteTable(con,"observations",new_observations,overwrite=TRUE)




# list all tables in the database
dbListTables(con)

#Close database connection ----
dbDisconnect(con)


proc.time()-ptm #15s






# (insert meteo parameters into eea_pollutants table)----
if(FALSE){
  meteo_to_insert <- web_mapping %>% dplyr::filter(pollutantID>9000) %>%
                                          select(pollutantID,notation,label,eea_pollutant) %>%
                                          distinct()
  names(meteo_to_insert) <- c("id","notation","label","uri")
  
  #TODO update eea_pollutants table, check code...
  dbWriteTable(con,"eea_pollutants",value=meteo_to_insert,append=TRUE,overwrite=FALSE)
}
