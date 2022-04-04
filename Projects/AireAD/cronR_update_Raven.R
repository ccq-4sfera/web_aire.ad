library(cronR)

# cron_ls() #list all cron jobs for this user


# cmd <- "cd '/var/rprojects/RScripts/' && /usr/lib/R/bin/Rscript './raven_database_lang_v2.R'  >> './raven_database_lang_v2.log' 2>&1"

cmd <- "cd '/var/rprojects/RScripts/' && /usr/lib/R/bin/Rscript './CopyOfraven_database_lang_v2_table.R'  >> './raven_database_lang_v2_table.log' 2>&1"

cron_add(command = cmd, frequency = 'hourly', at = '00:12', 
         id = 'update_Raven', 
         description = 'This gets hourly data for Andorra (updated at HH:11) and updates Raven tables and saves txt files with future tweets',
         tags = c('AD', 'raven', 'update', 'hourly'))
