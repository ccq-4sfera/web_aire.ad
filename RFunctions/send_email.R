# send a mail

# 1. "no_data" 
#     Si no arriben dades, enviar mail a Andorra i a nosaltres (1 vegada només)
# 2. "restablished" 
#     Quan es tornin a rebre dades, enviar mail a Andorra i a nosaltres
# 3. "fail" 
#     Si han arribat dades però ha fallat script enviar mail a nosaltres
# 4. "no_update" 
#     Si han arribat dades, s'ha executat correctament l'script però no hi ha hagut actualitzacions (mirar per ex. taula web_latest_data) enviar mail a nosaltres

library(emayili)

send_email <- function(type_of_mail, extra_argument=NULL){
  
  # PARAMETERS ---
  # type_of_mail <- "no_data" #"no_data", "restablished", "fail" or "no_update"
  sender <- "aireandorra@4sfera.eu"
  # ---

  credentials_file <- read.delim("/var/rprojects/.mail_credentials",
                                 sep = "=",header=F)
  credentials_file <- as.data.frame(t(credentials_file))
  names(credentials_file) <- credentials_file[1,]
  credentials_file <- credentials_file[-1,]
  
  
  dtime <- as.character(format(Sys.time(),"%d/%m/%Y %H:%M"))
  
  if(type_of_mail == "no_data"){
    recipients <- c("cristina.carnerero@4sfera.com","jaume.targa@4sfera.com")
    
    email_subject <- "ALERTA aire.ad no actualitzada"
    email_body <- paste0("Us informem que a data ",dtime," no estem rebent les dades per actualitzar la web aire.ad. Aquest correu es reenviarà cada hora si l'error persisteix. Us enviarem un altre correu quan tornem a rebre dades. Aquesta comprovació es fa un cop cada hora.")
    
  }else if(type_of_mail == "restablished"){
    recipients <- c("cristina.carnerero@4sfera.com","jaume.targa@4sfera.com")

    email_subject <- "aire.ad torna a estar actualitzada"
    email_body <- paste0("Us informem que a data ",dtime," s'ha reestablert la recepció de dades per actualitzar la web aire.ad. A partir d'aquest moment la web està mostrant les dades més recents.")
    
  }else if(type_of_mail == "fail"){
    recipients <- c("cristina.carnerero@4sfera.com","jaume.targa@4sfera.com")
    
    email_subject <- "ERROR backend aire.ad"
    # email_body <- paste0("Hi ha hagut un error en actualitzar les bases de dades al Raven i la web no està mostrant dades actualitzades a data ",dtime,". Comproveu l'script 'raven_database_lang_v2.R' i el fitxer 'update_raven.log'. Aquest correu es reenviarà cada hora si l'error persisteix.")
    email_body <- paste0("Hi ha hagut un error al processar les dades rebudes a data ",dtime,". No s'han actualitzat les bases de dades al Raven i la web no està mostrant dades actualitzades. Comproveu la taula '/var/rprojects/Projects/AireAD/table_errors.csv'. Aquest correu es reenviarà cada hora si l'error persisteix.")
    
  }else if(type_of_mail == "no_update"){
    recipients <- c("cristina.carnerero@4sfera.com")
    
    email_subject <- "ERROR backend aire.ad"
    email_body <- paste0("Després de processar les dades horàries rebudes, no s'han actualitzat les bases de dades al Raven i la web no està mostrant dades actualitzades a data ",dtime,". Comproveu la taula '/var/rprojects/Projects/AireAD/table_errors.csv'. Aquest correu es reenviarà cada hora si l'error persisteix. \n ",extra_argument)
    
  }else if(type_of_mail == "pollen"){
    recipients <- c("cristina.carnerero@4sfera.com","jaume.targa@4sfera.com")
    
    #TODO elaborar mail... 
    
    email_subject <- "Pol·len processat"
    email_body <- paste0("El fitxer '",extra_argument, "' s'ha processat correctament a data ",dtime,".")
  }else if(type_of_mail == "alive"){
    recipients <- c("cristina.carnerero@4sfera.com","jaume.targa@4sfera.com")
    
    email_subject <- "Raven aire.ad viu"
    email_body <- extra_argument
  }
  
  # construct mail
  email <- envelope(
    to = recipients,
    from = sender,
    subject = email_subject,
    text = email_body)
  
  # server
  smtp <- server(
    host = credentials_file$smtp_host,
    port = credentials_file$smtp_port,
    username = credentials_file$smtp_user,
    password = credentials_file$smtp_pwd)
  
  # send mail
  smtp(email, verbose = F)
  
  return()
}