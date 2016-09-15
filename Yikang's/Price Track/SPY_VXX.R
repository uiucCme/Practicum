require(quantmod)
require(ggplot2)
options(digits = 12)
setwd("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")
SPY <- read.table(paste(getwd(), "/data/price/SPY_price_track.txt", sep = ''), 
                  sep = '\t',header = TRUE, nrow = FALSE, stringsAsFactors = FALSE)
VXX <- read.table(paste(getwd(), "/data/price/VXX_price_track.txt", sep = ''), 
                  sep = '\t',header = TRUE, nrow = FALSE, stringsAsFactors = FALSE)

time.convert <- function(x){
        group <- strsplit(x, ":")[[1]]
        h <- group[1]
        m <- group[2]
        s <- group[3]
        as.numeric(h)*3600 + as.numeric(m)*60 + as.numeric(s)
}
return.get <- function(x){
        ROC(x, type = 'discrete')
}

SPY$Return <- return.get(SPY$Price)
VXX$Return <- return.get(VXX$Price)
SPY$Time <- sapply(SPY$Time, time.convert)
VXX$Time <- sapply(VXX$Time, time.convert)
merged <- merge(SPY[c('Time', 'Return')], VXX[c('Time', 'Return')],
                by = 'Time', 
                all = TRUE,
                suffixes = c('.SPY', '.VXX'))
merged[is.na(merged)] <- 0

correlation_test <- function(lag){
        time <- merged[,1]
        vec1 <- merged[,2]
        vec2 <- c(rep(NA, lag), merged[,3])[1:nrow(merged)]
        time_gap <- time[lag+1] - time[1]
        c(time_gap, cor(vec1, vec2, use = "complete"))
}

result <- data.frame(t(mapply(correlation_test, 1:10000)))
result[which(result$X2 == min(result$X2)),]

ggplot(data = result, aes(x=X1, y=X2))+ labs(x = 'Lag(s)', y = 'Correlation') + geom_line()
