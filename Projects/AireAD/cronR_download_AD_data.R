library(cronR)

# cron_ls() #list all cron jobs for this user

# cmd <- "cd '/var/rprojects/RScripts/' && /usr/lib/R/bin/Rscript './read_obs_AD.R'  >> './download_AD_obs.log' 2>&1"

cmd <- "cd '/var/rprojects/RScripts/' && /usr/lib/R/bin/Rscript './read_obs_AD_v2.R'  >> './download_AD_obs.log' 2>&1"
cron_add(command = cmd, frequency = 'hourly', at = '00:11', 
         id = 'download_AD_obs', 
         description = 'This gets hourly data for Andorra (updated at HH:10) and updates the dataframe "Andorra_hourly.RDS" in DATA',
         tags = c('AD', 'download', 'hourly'))
