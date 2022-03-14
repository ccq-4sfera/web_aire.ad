# Read hourly data and transform it to dataframe similar to observations table in raven

library(lubridate)
library(openxlsx)
library(dplyr)


info_file <- "/var/rprojects/Projects/AireAD/Andorra_info.xlsx"
input_file <- "/home/andorra/uploads/ftp/Andorra.txt"
temporal_file <- "/var/rprojects/DATA/tmp/Andorra.txt"
output_rds <- "/var/rprojects/DATA/Andorra_hourly.RDS"
output_csv <- "/var/rprojects/DATA/Andorra_hourly.csv"


# Read file ----
#file is updated hourly at HH:10
meta <- read.table(input_file,sep=";",dec=",",header=F,nrows=1,
                   col.names = c("time_upload","first_datetime","last_datetime","freq","comment"))
# headers <- read.table(input_file,sep=";",dec=",",header=F,skip=1,nrows=1)
txt <- read.table(input_file,sep=";",header=F,skip=2,dec=",",
                  col.names=c("Organisme","Station","Mesure","Constituant","Unite","Date","Valeur","Etat"))

# move file to a temp directory
file.rename(from=input_file, to=temporal_file)


# Process file ----
# we need a dataframe with the same structure as "observations" table in Raven
begin_position <- as.POSIXct(txt$Date, format="%d/%m/%Y %H:%M", tz="UTC+1")
end_position <- begin_position+dhours(1)
begin_position_fmt <- format.Date(begin_position,"%Y-%m-%dT%H:%M:%S+01:00")
end_position_fmt <- format.Date(end_position,"%Y-%m-%dT%H:%M:%S+01:00")

# if Etat is A or R, validation_flag=1, otherwise -1
validation_flag <- ifelse(txt$Etat=="A" | txt$Etat=="R",1,-1)
verification_flag = 3


# translate Constituent to pollutant code
web_RavenMapping <- read.xlsx(info_file,"web_RavenMapping")
poll <- web_RavenMapping[match(txt$Constituant,web_RavenMapping$Constituant),'pollutantID']
spo <- paste0("SPO-AD0",txt$Station,"A-",poll)

# Create dataframe with latest hourly concentrations
df <- data.frame("sampling_point_id"=spo,
                 "begin_position"=begin_position_fmt,
                 "end_position"=end_position_fmt,
                 "value"=txt$Valeur,
                 "verification_flag"=verification_flag,
                 "validation_flag"=validation_flag)
                 

# add columns to match observations table in raven
df$touched <- now(tz="Europe/Andorra")
df$from_time <- as.POSIXct(df$begin_position,"%Y-%m-%dT%H:%M:%S+01:00",tz="UTC+01")
df$to_time <- as.POSIXct(df$end_position,"%Y-%m-%dT%H:%M:%S+01:00",tz="UTC+01")
df$import_value <- df$value

#filter rows corresponding to future values
df <- df %>% dplyr::filter(to_time<now(tz="Europe/Andorra"))

# transform NAs to -999
# df[is.na(df)] <- -999

# Upload saved dataframe ----
# read old dataframe if it exists, append new rows and save with the same name
if(file.exists(output_rds)){
  output_old <- readRDS(output_rds)
  output_new <- output_old %>% full_join(df)
  
  saveRDS(output_new,output_rds)
  write.csv(output_new,output_csv)
}else{
# if df does not exist, save it now (only first time)
  saveRDS(df,output_rds)
  write.csv(df,output_csv)
}


# Remove temp file
# file.remove(temporal_file)

