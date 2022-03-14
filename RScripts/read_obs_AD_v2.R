# Read hourly data and transform it to dataframe similar to observations table in raven
# semi-prepared to read also validated data

library(lubridate)
library(openxlsx)
library(dplyr)
library(RCurl)

#All dates in the input and output files are UTC+1
read_obs <- function(input_file="/home/andorra/uploads/ftp/Andorra.txt",
                     temporal_file="/var/rprojects/DATA/tmp/Andorra.txt",
                     output_rds="/var/rprojects/DATA/Andorra_hourly.RDS",
                     output_csv="/var/rprojects/DATA/Andorra_hourly.csv",
                     info_file="/var/rprojects/Projects/AireAD/Andorra_info.xlsx",
                     verification_flag=3){
  
  time_begin <- as.character(Sys.time())
  
    #check if the file exists in that directory
  file_found <- file.exists(input_file)
  
  if(file_found){
    print("New file found")
    
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
    df$touched <- now(tzone="Etc/GMT-1")
    
    df$from_time <- begin_position
    df$to_time <- end_position
    
    df$import_value <- df$value
    
    #filter rows corresponding to future values
    df <- df %>% dplyr::filter(to_time<touched)
    
    # transform NAs to -999
    # df[is.na(df)] <- -999
    
    # Upload saved dataframe ----
    # read old dataframe if it exists, append new rows and save with the same name
    if(file.exists(output_rds)){
      output_old <- readRDS(output_rds)
      
      # merge old with new data
      output_new <- output_old %>% full_join(df, by=c("sampling_point_id","begin_position", 
                                                      "end_position","value","verification_flag", 
                                                      "validation_flag","touched","from_time", 
                                                      "to_time","import_value"))

      
      # if there are are validated results (verification_flag=1), keep the new values and order by date
      output_new <- output_new %>% 
                      group_by(begin_position) %>% 
                      dplyr::filter(verification_flag==min(verification_flag)) %>%
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
    
    # if file is validated save with a different name
    if(temporal_file=="/var/rprojects/DATA/tmp/Andorra_validated.txt"){
      file.copy(from=temporal_file,
                  to=paste0("/var/rprojects/DATA/Andorra_historic/Andorra_validated_",file_date,".txt"))
    }else{
      # upload file to Ricardo FTP (old web)
      try(ftpUpload(temporal_file, "ftps://ftps.ricardo-aea.com/Andorra.txt", userpwd = "andorra:thatch3lime"))
      
      # move file from temp directory to historic directory
      file.copy(from=temporal_file,
                to=paste0("/var/rprojects/DATA/Andorra_historic/Andorra_",file_date,".txt"))
    }
    
    print("OK!")
    
  }else{
    print("Error: New file not found")
  }
  
  time_end <- as.character(Sys.time())
  
  
  # create table (EXCEPT when we are looking for validated files and do not find them)
  if(input_file=="/home/andorra/uploads/ftp/Andorra.txt" | (input_file=="/home/andorra/uploads/ftp/validades/Andorra.txt" & file.exists(input_file))){
    
    table_out <- data.frame("time_begin" = time_begin,
                            "file_found" = file_found,
                            "time_file" = ifelse(file_found,file_time,NA),
                            "rows" = ifelse(file_found,nrow(txt),NA),
                            "time_end" = time_end,
                            "error_script" = FALSE)
    
    write.csv(table_out, file = "/var/rprojects/Projects/AireAD/table_errors.csv", append = T)
    #TODO change table_out$error_script when script fails
    
  }
  
}


# EXECUTE FUNCTIONS----
# Hourly data
read_obs()

# Check if there is validated data and update the data
read_obs(input_file="/home/andorra/uploads/ftp/validades/Andorra.txt",
         temporal_file="/var/rprojects/DATA/tmp/Andorra_validated.txt",
         verification_flag = 1)

