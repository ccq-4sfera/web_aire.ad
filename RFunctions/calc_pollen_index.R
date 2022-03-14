# Calculate pollen index

calc_pollen_index <- function(df,ref_index){

  for(p in ref_index$species){
    index_pol <- ref_index %>% dplyr::filter(species %in% p)
    
    index_range <- list(index_pol$a, index_pol$b, index_pol$c, index_pol$d, index_pol$e)
    
    # calc index by assigning integer to range number
    df[df$species==p,"index"] <- cut(df$value, breaks=index_range, labels=FALSE, include.lowest=TRUE, right=FALSE)[which(df$species==p)]
  }
  
  return(df)
}