#patrons diaris AD

# DB Raven
database = "ravendb"
host = "localhost"
port = 5432
user = "ravendb"
pwd = "ravendb"
# Connect to the database
con <- dbConnect(RPostgres::Postgres(),dbname=database,host=host,port=port,user=user,password=pwd)


# df <- readRDS("/var/rprojects/DATA/Andorra_hourly.RDS")
df <- dbReadTable(con,"observations")


#PROCESS ----
Andorra_info_file = "/var/rprojects/Projects/AireAD/Andorra_info.xlsx"
list.SPO <- read_excel(Andorra_info_file,"web_RavenMapping")

df$station <- substr(df$sampling_point_id,5,11)
df$pollutant <- substr(df$sampling_point_id,13,nchar(df$sampling_point_id))

#filter meteo parameters
df <- df %>% dplyr::filter(pollutant<9000)

df$pollutant <- list.SPO[match(df$pollutant,list.SPO$pollutant_id),"notation"] %>% pull


# for duplicated rows, keep the one with value (not NA)
df_all <- df  %>% group_by(end_position) %>%
  group_by(sampling_point_id,.add=T) %>%
  slice(which.max(!is.na(value)))

names(df_all)[names(df_all)=="end_position"] <- "date"  #date is end_time in UTC+1

df_all$date <- as.POSIXct(df_all$date,"%Y-%m-%dT%H:%M:%S+01:00",tz="Europe/Andorra")

# filter concentration >-10
df_all <- df_all %>% dplyr::filter(value>-10) %>% ungroup %>% select(date,station,pollutant,value)

# convert dataframes to have pollutants as columns
data_col <- df_all %>% pivot_wider(id_cols=c(station,date),names_from=pollutant)



data_col_NO2_942 <- df_all %>% dplyr::filter(pollutant=="NO2") %>% dplyr::filter(station=="AD0942A") %>% select(-pollutant)
timeVariation(data_col_NO2_944,pollutant="value",main="Escaldes-Engordany",name.pol="NO2")

data_col_O3_944 <- df_all %>% dplyr::filter(pollutant=="O3") %>% dplyr::filter(station=="AD0944A") %>% select(-pollutant)
timeVariation(data_col_O3_944,pollutant="value",main="Engolasters",name.pol="O3")


#PLOTS----
plot_station <- function(station,pols){
  st <- paste0("AD0",station,"A")
  df <- data_col %>% dplyr::filter(station==station) %>% select(where(~!all(is.na(.x))))
  
  # fname <- paste0("/var/rprojects/Projects/AireAD/plots/TimeVariation_",station,".png")
  # png(filename=fname,width=1000,height=650,res=120)
  timeVariation(df,pollutant = pols, main=station)
  # dev.off()
}

plot_station("940",c("NO2","PM2.5"))
plot_station("942",c("NO2","O3","PM10"))
plot_station("943",c("NO2"))
plot_station("944",c("O3"))

