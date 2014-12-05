source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)


mgp <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[mgp_state_longitudinal]")

minmax <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[mgp_minmax]")

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

.json <- toJSON(.nested_list, na = 'null')
.json <- gsub("[[","",.json, fixed=TRUE)
.json <- gsub("]]","",.json, fixed=TRUE)

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
