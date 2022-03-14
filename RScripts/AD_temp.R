
library(RPostgres)
library(DBI)
library(tidyr)
library(openxlsx)
library(httr)

# PARAMETERS DB ----
database = "ravendb"
host = "localhost"
port = 5432
user = "ravendb"
pwd = "ravendb"
# ---
AD_hourly_csv <- "/var/rprojects/DATA/Andorra_hourly.csv"
AD_hourly_RDS <- "/var/rprojects/DATA/Andorra_hourly.RDS"
info_file = "/var/rprojects/Projects/AireAD/Andorra_info.xlsx"


# Connect to the database ----
con <- dbConnect(RPostgres::Postgres(), dbname=database, host=host, port=port, user=user, password=pwd)

Andorra_hourly <- readRDS(AD_hourly_RDS)

#CHECK drop duplicated rows
# Andorra_hourly <- Andorra_hourly %>% group_by(begin_position) %>% filter(!is.na(value))

#TODO filter stations in observing_capabilities
# Andorra_to_observations <- Andorra_hourly %>% dplyr::filter(sampling_point_id %in% list.SPO$sampling_point_id)


dbWriteTable(con,"web_observations",value=Andorra_hourly,overwrite=TRUE)

#TODO update? 
#? dbBind(new,old) #this will only work if we have a key (e.g. row names) that match in both dfs


#upload last csv to raven as a table with name "web_observations"
POST("http://116.203.249.237:8888/imports/web_observations", authenticate("admin", "admin"), 
     body = list(csv = upload_file(AD_hourly_csv)), 
     verbose())



#TODO web tables... (./raven_database)

# list all tables in the database
dbListTables(con)

#Close database connection ----
dbDisconnect(con)
