# read pollen data, save it to file with historic data and calulate index (from weekly average) to show on website
# 13/01/2022


process_pollen <- function(new_data_file = "/var/rprojects/DATA/polen_example.csv",
                           historic_file = "/var/rprojects/DATA/all_pollen_data.xlsx",
                           index_thresholds = "/var/rprojects/Projects/AireAD/pollen_index_thresholds.xlsx",
                           Andorra_info_file = "/var/rprojects/Projects/AireAD/Andorra_info.xlsx",
                           output_file = "/var/rprojects/DATA/weekly_pollen.csv"){
  
  # FUNCTIONS ----
  source("/var/rprojects/RFunctions/calc_pollen_index.R")
  # ---
  
  
  #1. Read new weekly data ----
  df <- read.csv(new_data_file, header=F)
  names(df) <- c("site","network","pid","start_date","end_date","measurement","below_LOD","ratid","uid","mpid")
  
  week_start_date <- df$start_date[1]
  week_end_date <- df$end_date[nrow(df)]
  
  #if ratid is 2 or 4, convert to missing
  df[df$ratid==2 | df$ratid==4,'measurement'] <- NA
  
  # convert date and select only columns of interest
  df <- df %>% mutate(measurement_date=as.POSIXct(start_date,format="%d/%m/%Y",tz="UTC")) %>% 
               select(measurement_date,pid,measurement)
  
  # delete white spaces after names
  df <- df %>% mutate(pid=trimws(pid))
  
  # transform df to dataframe with species as columns and dates as rows
  df_cols <- df %>% pivot_wider(names_from=pid,values_from=measurement)
  
  
  #2. Update historic data ----
  # read file with historic data
  historic <- read_excel(historic_file) %>% 
              mutate(measurement_date=as.POSIXct(measurement_date,format="%Y-%m-%d %H:%M:%S"))
  
  # update historic data with new data
  historic_updated <- historic %>% bind_rows(df_cols) %>% 
                      mutate(measurement_date = date(measurement_date)) %>%  #keep only the date, drop the hour
                      group_by(measurement_date) %>%
                      dplyr::filter(row_number()==n()) # in duplicated rows: keep newest row
  
  #FIXME? save updated historic data
  write.xlsx(historic_updated, file=historic_file, rowNames=F)
  
  
  #3. Calculate index ----
  # calculate weekly averages
  df_weekly <- summarise_all(df_cols[,-1], mean, na.rm=T) %>% 
               pivot_longer(cols=everything(),names_to="species")
  
  # read index threshold table created from http://www.aire.ad/pollen.php?n_action=levels
  thresholds <- read_excel(index_thresholds, sheet="index")
  
  # calculate index from weekly averages
  web_pollen_weekly <- calc_pollen_index(df_weekly,thresholds) %>% select(-value)
  
  
  
  web_pollen_mapping <- read.xlsx(Andorra_info_file,"web_pollen")
  
  #column specifying if pollen or spore
  web_pollen_weekly$type <- web_pollen_mapping[match(web_pollen_weekly$species,web_pollen_mapping$species),'type']
  #column with name in html
  web_pollen_weekly$name_CAT <- web_pollen_mapping[match(web_pollen_weekly$species,web_pollen_mapping$species),'web_name_CAT']
  #start and end of week
  web_pollen_weekly$start_date <- week_start_date
  web_pollen_weekly$end_date <- week_end_date
 
  write.csv(web_pollen_weekly, output_file)
  

  return(web_pollen_weekly) 
}
