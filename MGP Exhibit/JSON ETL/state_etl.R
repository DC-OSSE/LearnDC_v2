setwd("X:/Learn DC/State Equity Report Development")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)


mgp <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[mgp_state_longitudinal]")
minmax <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[mgp_minmax]")
mgp <- merge(mgp, minmax, by = c("year","subgroup","subject"), all.x=TRUE)

mgp$population <- "All"

mgp_gen <- mgp
mgp_gen$population <- "Gen"
mgp_gen$NSize <- round(mgp_gen$NSize * .75,0)
mgp_gen$mgp_1yr <- round(mgp_gen$mgp_1yr * .75,0)
mgp_gen$mgp_2yr <- round(mgp_gen$mgp_2yr * .75,0)
mgp_gen$min_mgp <- round(mgp_gen$min_mgp * .75,0)
mgp_gen$max_mgp <- round(mgp_gen$max_mgp * .75,0)

mgp_alt <- mgp
mgp_alt$population <- "Alt"
mgp_alt$NSize <- round(mgp_alt$NSize * .25,0)
mgp_alt$mgp_1yr <- round(mgp_alt$mgp_1yr * .25,0)
mgp_alt$mgp_2yr <- round(mgp_alt$mgp_2yr * .25,0)
mgp_alt$min_mgp <- round(mgp_alt$min_mgp * .25,0)
mgp_alt$max_mgp <- round(mgp_alt$max_mgp * .25,0)

state_mgp_all <- rbind.data.frame(mgp,mgp_gen,mgp_alt)

strtable(state_mgp_all)

# setwd('U:/LearnDC ETL V2/Export/CSV/state')
# write.csv(mgp, "Equity_Report_MGP_State.csv", row.names=FALSE)

key_index <- c(1:3,9)
value_index <- c(5:8)

setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

.nested_list <- lapply(1:nrow(state_mgp_all), FUN = function(i){ 
  list(key = list(state_mgp_all[i,key_index]), 
       val = list(state_mgp_all[i,value_index]))
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