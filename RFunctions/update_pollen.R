library(mRpostman)
library(lubridate)


update_pollen <- function(con){

  #function to process pollen
  source("/var/rprojects/RFunctions/process_pollen.R")
  
  #function to send mail
  source("/var/rprojects/RFunctions/send_email.R")
  
  #load mail credentials
  credentials_file <- read.delim("/var/rprojects/.mail_credentials_gmail",
                                 sep = "=",header=F)
  credentials_file <- as.data.frame(t(credentials_file))
  names(credentials_file) <- credentials_file[1,]
  credentials_file <- credentials_file[-1,]
  
  # connect to the server
  mail_con <- configure_imap(
    url = "imaps://imap.gmail.com",
    username = credentials_file$smtp_user,
    password = credentials_file$smtp_pwd)
  
  # mail_con$list_server_capabilities()
  # mail_con$list_mail_folders()
  
  
  # mails containing "Pòl·lens" in the subject and with attachments are moved to folder "pollen"
  mail_con$select_folder(name = "pollen")
  
  
  # set date to one week ago
  search_from <- Sys.time() %m-% days(7) %>% format("%d-%b-%Y")
  
  
  # how many emails are there since given date (returns meassages ids)
  ids <- mail_con$search_since(date_char = search_from)
  
  if(!is.na(ids)){
    for(message_id in ids){
      # download attachment
      mail_con$fetch_body(msg_id=message_id, write_to_disk = FALSE, use_uid=FALSE) %>% mail_con$get_attachments()
      
      # load attachment
      input_filename <- list.files(paste0("./aireandorra@gmail.com/pollen/",message_id), full.names=T, pattern="csv")
      
      # if there are more than one files, take the first (by decreasing order, i.e., most recent date)
      if(length(input_filename>1)){
        input_filename <- sort(input_filename, decreasing = T)[1]
      }
      
      # process pollen file
      web_pollen_weekly <- process_pollen(new_data_file = input_filename)
      
      # update Raven table "web_pollen_weekly"
      dbWriteTable(con,"web_pollen_weekly",web_pollen_weekly,overwrite=TRUE)
      
      
      # clear our local folder (move file to another directory with processed files)
      end_filename <- paste0("/var/rprojects/DATA/Processed_pollen/",basename(input_filename))
      file.rename(from=input_filename, to=end_filename)
      
      # delete all other files in the local folder
      unlink(dirname(input_filename), recursive = TRUE)
      
      
      # move mail to folder pollenprocessed
      mail_con$move_msg(message_id, to_folder = "pollenprocessed")
      
      # change folder again to "pollen"
      mail_con$select_folder(name = "pollen")
      
      
      # send a mail when finished processing correctly
      send_email("pollen",basename(input_filename))
      
    }
    return("New pollen file processed")
    
  }else{
    return("No new pollen file found")
  }

}

