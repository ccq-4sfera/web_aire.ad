# post a Tweet in Twitter

library(twitteR)

# tweet_file <- "/var/rprojects/Projects/AireAD/Tweets/tweet_temp.txt"
# tweet_file <- "/var/rprojects/Projects/AireAD/Tweets/tweet_AQI.txt"

post_tweet <- function(tweet_text){
  
  # read account credentials from file
  credentials_file <- read.delim("/var/rprojects/Projects/AireAD/Tweets/twitter_credentials.txt",
                                 sep = " ",header=F)
  credentials_file <- as.data.frame(t(credentials_file))
  names(credentials_file) <- credentials_file[1,]
  credentials_file <- credentials_file[-1,]

  origop <- options("httr_oauth_cache")
  options(httr_oauth_cache=TRUE)
  
  #setup account
  setup_twitter_oauth(consumer_key = credentials_file$consumer_key,
                      access_token = credentials_file$token,
                      consumer_secret = credentials_file$consumer_secret,
                      access_secret = credentials_file$secret)
  
  options(httr_oauth_cache=origop)
  
  
  #read last tweet generated
  # tweet_text <- read.table(tweet_file)
  
  #post tweet
  tw <- updateStatus(tweet_text)
  
  return()
}
