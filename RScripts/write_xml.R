# NOTE: This must be called AFTER the Tweet
# Write XML in RSS format

library(XML)
library(dplyr)



# GET INDEX AND BANDS----
#global
band_global <- countryAQ$aqi_band_CAT #this is created when writing the Tweet text
index_global <- floor(countryAQ$aqi)

#escaldes
band_942 <- table3 %>% dplyr::filter(station_id=="AD0942A") %>% ungroup %>% select(aqi_band_CAT) %>% pull
index_942 <- table3 %>% dplyr::filter(station_id=="AD0942A") %>% ungroup %>% select(aqi) %>% pull %>% floor()

#engolasters
band_944 <- table3 %>% dplyr::filter(station_id=="AD0944A") %>% ungroup %>% select(aqi_band_CAT) %>% pull
index_944 <- table3 %>% dplyr::filter(station_id=="AD0944A") %>% ungroup %>% select(aqi) %>% pull %>% floor()

#sud radio
band_945 <- table3 %>% dplyr::filter(station_id=="AD0945A") %>% ungroup %>% select(aqi_band_CAT) %>% pull
index_945 <- table3 %>% dplyr::filter(station_id=="AD0945A") %>% ungroup %>% select(aqi) %>% pull %>% floor()

lastUpdate_fmt <- format(lastUpdate, format = "%a, %d %b %Y %H:%M:%S %z")

# CREATE XML FILE ----
doc = newXMLDoc()
# root = newXMLNode("rss", doc = doc, attrs = c(version="2.0"), namespace = c(xmlns="http://www.w3.org/2005/Atom"))
root = newXMLNode("rss", doc = doc, attrs = c(version="2.0"))

# WRITE XML NODES AND DATA
channel = newXMLNode("channel", parent = root)
description = newXMLNode("description","Qualitat de l'Aire a Andorra  - últimes dades", parent = channel)
title = newXMLNode("title","Qualitat de l'Aire a Andorra", parent = channel)
link = newXMLNode("link","http://www.aire.ad/", parent = channel)
language = newXMLNode("language","ca", parent = channel)
copyright = newXMLNode("copyright","(c) 4sfera Innova SL", parent = channel)
webMaster = newXMLNode("webMaster","unitat_medi_atmosferic@govern.ad (Unitat Medi Atmosfèric)", parent = channel)
lastBuildDate = newXMLNode("lastBuildDate",as.character(format(Sys.time(), format = "%a, %d %b %Y %H:%M:%S %z")), parent = channel)

image = newXMLNode("image", parent = channel)
url = newXMLNode("url","http://www.aire.ad/assets/img/banner.png", parent = image)
title = newXMLNode("title","Qualitat de l'Aire a Andorra", parent = image)
link = newXMLNode("link","http://www.aire.ad/", parent = image)

# image = newXMLNode("image", parent = channel)
# url = newXMLNode("url","http://www.aire.ad/assets/img/LOGO-VERD-OK.png", parent = image)
# title = newXMLNode("title","4sfera Innova", parent = image)
# link = newXMLNode("link","http://www.4sfera.com/", parent = image)


#INDEX GLOBAL
item = newXMLNode("item", parent = channel)
title = newXMLNode("title","Índex global de qualitat de l'aire a Andorra", parent = item)
link = newXMLNode("link","http://www.aire.ad/", parent = item)
guid = newXMLNode("guid","http://www.aire.ad/", parent = item)
description = newXMLNode("description",paste0("La qualitat de l'aire és ",band_global," - índex ",index_global,"/6"), parent = item)
pubDate = newXMLNode("pubDate",as.character(lastUpdate_fmt), parent = item)

#ESCALDES
item = newXMLNode("item", parent = channel)
title = newXMLNode("title","Escaldes-Engordany", parent = item)
link = newXMLNode("link","http://www.aire.ad/station?station=AD0942A", parent = item)
guid = newXMLNode("guid","http://www.aire.ad/station?station=AD0942A", parent = item)
description = newXMLNode("description",paste0("La qualitat de l'aire és ",band_942," - índex ",index_942,"/6"), parent = item)
pubDate = newXMLNode("pubDate",as.character(lastUpdate_fmt), parent = item)

#ENGOLASTERS
item = newXMLNode("item", parent = channel)
title = newXMLNode("title","Engolasters", parent = item)
link = newXMLNode("link","http://www.aire.ad/station?station=AD0944A", parent = item)
guid = newXMLNode("guid","http://www.aire.ad/station?station=AD0944A", parent = item)
description = newXMLNode("description",paste0("La qualitat de l'aire és ",band_944," - índex ",index_944,"/6"), parent = item)
pubDate = newXMLNode("pubDate",as.character(lastUpdate_fmt), parent = item)

#SUD RADIO
item = newXMLNode("item", parent = channel)
title = newXMLNode("title","Sud Ràdio", parent = item)
link = newXMLNode("link","http://www.aire.ad/station?station=AD0945A", parent = item)
guid = newXMLNode("guid","http://www.aire.ad/station?station=AD0945A", parent = item)
description = newXMLNode("description",paste0("La qualitat de l'aire és ",band_945," - índex ",index_945,"/6"), parent = item)
pubDate = newXMLNode("pubDate",as.character(lastUpdate_fmt), parent = item)



# OUTPUT XML CONTENT TO FILE----
saveXML(doc, file=paste0(RSS_path,"airead.xml"))
