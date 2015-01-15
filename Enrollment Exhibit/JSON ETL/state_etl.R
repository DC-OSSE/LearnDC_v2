setwd("U:/LearnDC ETL V2/Enrollment Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)


state_enr <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[enrollment_state_exhibit]")

state_enr <- subset(state_enr, enrollment >= 10)


setwd('U:/LearnDC ETL V2/Export/CSV/state')
write.csv(state_enr, "Enrollment_State.csv", row.names=FALSE)


setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")



key_index <- c(1,2,3)
value_index <- 4


nested_list <- lapply(1:nrow(state_enr), FUN = function(i){ 
                         list(key = list(state_enr[i,key_index]), 
                         	val = list(state_enr[i,value_index]))
                       })

json <- toJSON(nested_list, na="null")
json <- gsub("[[","",json, fixed=TRUE)
json <- gsub("]]","",json, fixed=TRUE)
json <- prettify(json)



newfile <- file("enrollment.json", encoding="UTF-8")
sink(newfile)

cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "state",', sep="", fill=TRUE)
cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
cat('"org_code": "dc",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "enrollment",', fill=TRUE)
cat('\t"data": ',json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)

sink()
close(newfile)

## VALIDATE JSON
test <- readLines("enrollment.json")
validate(test)

