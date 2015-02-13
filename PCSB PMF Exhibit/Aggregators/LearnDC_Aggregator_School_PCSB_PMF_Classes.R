source("U:R/tomkit.R")
setwd("U:/LearnDC ETL V2/PCSB PMF Exhibit/Aggregators")

pmf_sy1314 <- sqlQuery(dbrepcard,"select * from dbo.pmf_sy1314")
pmf_sy1314$year <- 2014

urls <- subset(pmf_sy1314,select=c('school_code','link'))

pmf_sy1213 <- sqlQuery(dbrepcard,"select * from dbo.pmf_sy1213")
pmf_sy1213$year <- 2013
pmf_sy1213 <- merge(pmf_sy1213,urls,by=c('school_code'),all.x=TRUE)

pmf_sy1112 <- sqlQuery(dbrepcard,"select * from dbo.pmf_sy1112")
pmf_sy1112$year <- 2012
pmf_sy1112 <- merge(pmf_sy1112,urls,by=c('school_code'),all.x=TRUE)

pmf_sy1011 <- sqlQuery(dbrepcard,"select * from dbo.pmf_sy1011")
pmf_sy1011$year <- 2011
pmf_sy1011 <- merge(pmf_sy1011,urls,by=c('school_code'),all.x=TRUE)

pmf_all <- rbind(pmf_sy1011,pmf_sy1112,pmf_sy1213,pmf_sy1314)

##Create file of Schools with No PMF URLs
# pmf_no_url <- pmf_all[which(is.na(pmf_all$link)),]
# write.csv(pmf_no_url,"pmf_no_url.csv",row.names=FALSE)

##Remove observations for PMF URLs that were not included in PCSB Submission
pmf_all <- subset(pmf_all, !is.na(pmf_all$link))

##Insert file with previously
pmf_w_url <- read.csv("pmf_w_url.csv")

pmf_all <- rbind(pmf_all,pmf_w_url)

sqlSave(dbrepcard_prod, pmf_all, tablename = "pcsb_pmf_school_exhibit", append = FALSE, rownames=FALSE)