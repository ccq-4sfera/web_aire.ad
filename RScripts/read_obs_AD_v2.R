# Read hourly data and transform it to dataframe similar to observations table in raven
# semi-prepared to read also validated data

library(lubridate)
library(openxlsx)
library(dplyr)
library(RCurl)


source("/var/rprojects/RFunctions/send_email.R")    # function to send an email

#All dates in the input and output files are UTC+1
read_obs <- function(input_file="/home/andorra/uploads/ftp/Andorra.txt",
                     temporal_file="/var/rprojects/DATA/tmp/Andorra.txt",
                     output_rds="/var/rprojects/DATA/Andorra_hourly.RDS",
                     output_csv="/var/rprojects/DATA/Andorra_hourly.csv",
                     info_file="/var/rprojects/Projects/AireAD/Andorra_info.xlsx",
                     verification_flag=3){
  
  time_begin <- as.character(Sys.time())
  print(time_begin)
  
  #check if the file exists in that directory
  file_found <- file.exists(input_file)
  
  if(file_found){
    print(paste0("New file found: ",input_file))
    
    #read at what time the file was created
    file_time <- file.info(input_file)$ctime
    
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
    begin_position <- as.POSIXct(txt$Date, "%d/%m/%Y %H:%M", tz="Etc/GMT-1")
    end_position <- begin_position %m+% hours(1)
    begin_position_fmt <- format.Date(begin_position,"%Y-%m-%dT%H:%M:%S+01:00")
    end_position_fmt <- format.Date(end_position,"%Y-%m-%dT%H:%M:%S+01:00")
    
    # if Etat is A or R, validation_flag=1, otherwise -1
    validation_flag <- ifelse(txt$Etat=="A" | txt$Etat=="R",1,-1)
    verification_flag <- verification_flag
    
    
    # translate Constituent to pollutant code
    web_RavenMapping <- read.xlsx(info_file,"web_RavenMapping")
    poll <- web_RavenMapping[match(txt$Constituant,web_RavenMapping$Constituant),'pollutant_id']
    spo <- paste0("SPO-AD0",txt$Station,"A-",poll)
    
    # Create dataframe with latest hourly concentrations
    df <- data.frame("sampling_point_id"=spo,
                     "begin_position"=begin_position_fmt,
                     "end_position"=end_position_fmt,
                     "value"=txt$Valeur,
                     "verification_flag"=verification_flag,
                     "validation_flag"=validation_flag)
    
    
    # add columns to match observations table in raven
    # df$touched <- now("UTC") %m+% hours(1)
    df$touched <- now("Etc/GMT-1")
    
    df$from_time <- as.POSIXct(df$begin_position,"%Y-%m-%dT%H:%M:%S+01:00",tz="Etc/GMT-1")
    df$to_time <- as.POSIXct(df$end_position,"%Y-%m-%dT%H:%M:%S+01:00",tz="Etc/GMT-1")
    
    df$import_value <- df$value
    
    #filter rows corresponding to future values
    df <- df %>% dplyr::filter(to_time<touched)
    
    # transform NAs to -999
    # df[is.na(df)] <- -999
    
    # Upload saved dataframe ----
    # read old dataframe if it exists, append new rows and save with the same name
    if(file.exists(output_rds)){
      
      output_old <- readRDS(output_rds)
      
      output_old$touched <- force_tz(output_old$touched,tzone="Etc/GMT-1")
      
      # merge old with new data
      # output_new <- output_old %>% full_join(df, by=c("sampling_point_id","begin_position","end_position",
      #                                                 "value","verification_flag","validation_flag",
      #                                                 "touched","from_time","to_time","import_value"))

      output_new <- output_old %>% bind_rows(df)
      
      # if there are are validated results (verification_flag=1), keep the new values and order by date
      output_new <- output_new %>% 
                      group_by(begin_position) %>% 
                      group_by(sampling_point_id,.add=T) %>% 
                     # dplyr::filter(verification_flag==min(verification_flag)) %>%
                      dplyr::filter(touched==max(touched)) %>%
                      # slice(which.max(!is.na(touched))) %>%
                      dplyr::arrange(from_time)
      
      saveRDS(output_new,output_rds)
      write.csv(output_new,output_csv)
    }else{
      # if df does not exist, save it now (only first time)
      saveRDS(df,output_rds)
      write.csv(df,output_csv)
    }
    
    # save file with date in filename
    file_date <- format(Sys.time(),"%Y%m%dT%H")
    
    # save file in DATA with a name including the time
    if(temporal_file=="/var/rprojects/DATA/tmp/Andorra_validated.txt"){
      file.copy(from=temporal_file,
                  to=paste0("/var/rprojects/DATA/Andorra_historic/Andorra_validated_",file_date,".txt"))
      
    }else if(temporal_file=="/var/rprojects/DATA/tmp/Andorra24hores.txt"){
      file.copy(from=temporal_file,
                to=paste0("/var/rprojects/DATA/Andorra_historic/Andorra24hores_",file_date,".txt"))
      
    }else if(temporal_file=="/var/rprojects/DATA/tmp/Andorra24dies.txt"){
      #FIXME?
      file.copy(from=temporal_file,
                to=paste0("/var/rprojects/DATA/Andorra_historic/Andorra24dies_",file_date,".txt"))
    }else{ 
      # upload file to Ricardo FTP (old web)
      # try(ftpUpload(temporal_file, "ftps://ftps.ricardo-aea.com/Andorra.txt", userpwd = "andorra:thatch3lime"))
      
      # move file from temp directory to historic directory
      file.copy(from=temporal_file,
                to=paste0("/var/rprojects/DATA/Andorra_historic/Andorra_",file_date,".txt"))
    }
    
    print("OK!")
    
  }else{
    if(input_file=="/home/andorra/uploads/ftp/Andorra.txt"){
      print(paste0("Error: ",input_file," file not found"))
      
      # send mail with message we did not recieve data
      #TODO only if in the last hour we did found the file, send mail "no_data"
      send_email("no_data")
    }
  }
  
  time_end <- as.character(Sys.time())
  
  
  # if last hour we didn't have data but now we have, send email "restablished"
  old_table <- read.csv("/var/rprojects/Projects/AireAD/table_errors.csv")
  last_hour_file_found <- old_table[nrow(old_table),"file_found"]
  
  if(input_file=="/home/andorra/uploads/ftp/Andorra.txt"){
    if(!last_hour_file_found & file_found){
      send_email("restablished")
    }
  }
  
  # create table (EXCEPT when we are looking for validated files and do not find them)
  if(input_file=="/home/andorra/uploads/ftp/Andorra.txt" | (input_file=="/home/andorra/uploads/ftp/Andorra24hores.txt" & file_found) | (input_file=="/home/andorra/uploads/ftp/Andorra24dies.txt" & file_found) | (input_file=="/home/andorra/uploads/ftp/validades/Andorra.txt" & file_found)){
    
    table_out <- data.frame("time_begin" = time_begin,
                            "file_found" = file_found,
                            "time_file" = ifelse(file_found,as.character(file_time),NA),
                            "rows" = ifelse(file_found,nrow(txt),NA),
                            "time_end" = time_end,
                            "error_script" = FALSE)
    
    write.table(table_out, file = "/var/rprojects/Projects/AireAD/table_errors.csv", sep = ",",
                col.names = !file.exists("/var/rprojects/Projects/AireAD/table_errors.csv"), 
                row.names = F, append = T)
  }
  
}



# EXECUTE FUNCTIONS----

# Hourly data
read_obs()

# Cada dia a les 8:15 ens arriben dades de les ultimes 24 hores que poden estar corregides
read_obs(input_file="/home/andorra/uploads/ftp/Andorra24hores.txt",
         temporal_file="/var/rprojects/DATA/tmp/Andorra24hores.txt",
         verification_flag = 3)

# Cada dimarts a les 9:45 ens arriben dades dels ultims 24 dies que poden estar corregides
read_obs(input_file="/home/andorra/uploads/ftp/Andorra24dies.txt",
         temporal_file="/var/rprojects/DATA/tmp/Andorra24dies.txt",
         verification_flag = 3)

# INCORPORAR DADES VALIDADES
# read_obs(input_file="/home/andorra/uploads/ftp/validades/AndorraValidades.txt",
#          temporal_file="/var/rprojects/DATA/tmp/Andorra_validated.txt",
#          verification_flag = 1)
