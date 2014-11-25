setwd("U:/LearnDC ETL V2/MGP Exhibit/Aggregator")
source("U:/R/tomkit.R")

minmax <- read.csv("mgp_minmax.csv")

minmax$subject <- NA
minmax$subject[which(minmax$metric %in% c("CAS Math Growth","CAS Math 2-year Growth"))] <- "Math"
minmax$subject[which(minmax$metric %in% c("CAS Reading Growth","CAS Reading 2-year Growth"))] <- "Reading"

minmax1 <- subset(minmax, metric %in% c("CAS Math Growth","CAS Reading Growth"))
minmax2 <- subset(minmax, metric %in% c("CAS Math 2-year Growth","CAS Reading 2-year Growth"))

minmax1$metric <- NULL
minmax2$metric <- NULL

colnames(minmax1)[3] <- "min_score1"
colnames(minmax1)[4] <- "max_score1"
colnames(minmax2)[3] <- "min_score2"
colnames(minmax2)[4] <- "max_score2"


df <- merge(minmax1, minmax2, by = c("school_year","student_group","subject"))



df$minscore <- NA
df$maxscore <- NA

df$min_score1 <- as.numeric(df$min_score1)
df$min_score2 <- as.numeric(df$min_score2)
df$max_score1 <- as.numeric(df$max_score1)
df$max_score2 <- as.numeric(df$max_score2)


for(i in 1:nrow(df)){

	min_list <- c(df$min_score1[i], df$min_score2[i])
	max_list <- c(df$max_score1[i], df$max_score2[i])
	df$minscore[i] <- min_list[which.min(min_list)]
	df$maxscore[i] <- max_list[which.max(max_list)]
}



library(dplyr)
final <- select(df, school_year, student_group, subject,minscore,maxscore)


sqlSave(dbrepcard, final, tablename = "mgp_minmax1", rownames=FALSE)