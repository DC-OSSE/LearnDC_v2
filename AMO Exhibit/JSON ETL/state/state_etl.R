setwd("U:/LearnDC ETL V2/DC CAS Exhibit/JSON ETL")
source("U:/R/tomkit.R")
library(dplyr)
library(jsonlite)

state_amo <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[amo_state_targets]")


state_amo$enrollment_status <- "full_year"
state_amo$grade <- "all"


state_amo_m <- select(state_amo, year, subgroup, grade, enrollment_status, math_baseline, math_target)
state_amo_r <- select(state_amo, year, subgroup, grade, enrollment_status, read_baseline, read_target)


state_amo_m$subject <- "Math"
state_amo_r$subject <- "Reading"
colnames(state_amo_m) <- c("year","subgroup","grade","enrollment_status","baseline","target","subject")
colnames(state_amo_r) <- c("year","subgroup","grade","enrollment_status","baseline","target","subject")

amo <- rbind(state_amo_m, state_amo_r)
amo <- select(amo, year, subgroup, subject, grade, enrollment_status, baseline, target)



setwd('U:/LearnDC ETL V2/Export/CSV/state')
write.csv(amo, "AMO_State.csv", row.names=FALSE)


setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")


key_index <- 1:5
value_index <- 6:7

nested_list <- lapply(1:nrow(amo), FUN = function(i){ 
                             list(key = list(amo[i,key_index]), 
                             	val = list(amo[i,value_index]))
                           })


json <- prettify(toJSON(nested_list, na="null"))


## write to file
newfile <- file("amo_targets.json", encoding="UTF-8")

sink(newfile)
cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "state",', sep="", fill=TRUE)
cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
cat('"org_code": "dc",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "amo_targets",', fill=TRUE)
cat('\t"data": ',json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)



sink()
close(newfile)

## VALIDATE JSON
test <- readLines("amo_targets.json")
validate(test)