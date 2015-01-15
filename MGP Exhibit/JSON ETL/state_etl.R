source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)


mgp <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[mgp_state_longitudinal]")

minmax <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[mgp_minmax]")

year <- c(2014,2014,2014,2014,2013,2013,2013,2013)
subject <- c('Math','Math','Reading','Reading','Math','Math','Reading','Reading')
subgroup <- c('FEMALE','MALE','FEMALE','MALE','FEMALE','MALE','FEMALE','MALE')
min_mgp <- c(10,8.5,15,8,1,1,7,2)
max_mgp <- c(85.5,93,83,86,91,92,86,99)

mm <- data.frame(year,subject,subgroup,min_mgp,max_mgp)

minmax = rbind(minmax,mm)

mgp <- merge(mgp, minmax, by = c("year","subgroup","subject"), all.x=TRUE)

# setwd('U:/LearnDC ETL V2/Export/CSV/state')
# write.csv(mgp, "Equity_Report_MGP_State.csv", row.names=FALSE)

key_index <- c(1,2,3)
value_index <- c(5,6,7,8)


setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")


.nested_list <- lapply(1:nrow(mgp), FUN = function(i){ 
                             list(key = list(mgp[i,key_index]), 
                             	val = list(mgp[i,value_index]))
                        })

.json <- toJSON(.nested_list, na="null")
.json <- gsub("[[","",.json, fixed=TRUE)
.json <- gsub("]]","",.json, fixed=TRUE)
.json <- prettify(.json)


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