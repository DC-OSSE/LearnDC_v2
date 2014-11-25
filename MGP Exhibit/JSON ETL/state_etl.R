setwd("U:/LearnDC ETL V2/Equity Report Exhibit/MGP JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

mgp_m <- sqlQuery(dbrepcard_prod, "SELECT [School_Year], [Student_Group], [Metric], [AverageScore]
	FROM [dbo].[equity_longitudinal] WHERE [Metric] in ('CAS Math Growth','CAS Math 2-year Growth')")

mgp_r <- sqlQuery(dbrepcard_prod, "SELECT [School_Year], [Student_Group], [Metric], [AverageScore]
	FROM [dbo].[equity_longitudinal] WHERE [Metric] in ('CAS Reading Growth','CAS Reading 2-year Growth')")

mgp_m$subject <- "Math"
mgp_r$subject <- "Reading"
mgp <- rbind(mgp_m, mgp_r)
mgp <- unique(mgp)


mgp$Metric[which(mgp$Metric %in% c("CAS Math Growth","CAS Reading Growth"))] <- "mgp_1yr"
mgp$Metric[which(mgp$Metric %in% c("CAS Math 2-year Growth","CAS Reading 2-year Growth"))] <- "mgp_2yr"

mgp_wide <- dcast(mgp, School_Year + Student_Group + subject ~ Metric, value.var = "AverageScore" )


colnames(mgp_wide) <- c("year","subgroup","subject","mgp_1yr","mgp_2yr")


mgp_wide$mgp_1yr[which(is.na(mgp_wide$mgp_1yr))] <- 'null'
mgp_wide$mgp_2yr[which(is.na(mgp_wide$mgp_2yr))] <- 'null'


# setwd('U:/LearnDC ETL V2/Export/CSV/state')
# write.csv(susp_wide, "Equity_Report_MGP_State.csv", row.names=FALSE)
key_index <- c(1,2,3)
value_index <- c(4,5)



setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")


.nested_list <- lapply(1:nrow(mgp_wide), FUN = function(i){ 
                             list(key = list(mgp_wide[i,key_index]), 
                             	val = list(mgp_wide[i,value_index]))
                        })

.json <- toJSON(.nested_list)
.json <- gsub("[[","",.json, fixed=TRUE)
.json <- gsub("]]","",.json, fixed=TRUE)
.json <- gsub('"null"','null',.json, fixed=TRUE)


newfile <- file("mgp_scores.json", encoding="UTF-8")
sink(newfile)

cat('{', fill=TRUE)
cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "state",', sep="", fill=TRUE)
cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
cat('"org_code": "dc",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "mgp_scores",', fill=TRUE)
cat('\t"data": ', .json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)

sink()
close(newfile)
