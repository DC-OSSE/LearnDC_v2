setwd("U:/LearnDC ETL V2/DC CAS Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)

state_cas <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[cas_state_exhibit]")

state_cas <- subset(state_cas, (enrollment_status == "full_year" & n_test_takers >= 25) | (enrollment_status == "all" & n_test_takers >= 10))


setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")


key_index <- 1:5
value_index <- 6:12

nested_list <- lapply(1:nrow(state_cas), FUN = function(i){ 
                             list(key = list(state_cas[i,key_index]), 
                             	val = list(state_cas[i,value_index]))
                           })


json <- toJSON(nested_list, na="null")
json <- gsub("[[","",json, fixed=TRUE)
json <- gsub("]]","",json, fixed=TRUE)
json <- prettify(json)



## write to file
newfile <- file("dccas.json", encoding="UTF-8")

sink(newfile)
cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "state",', sep="", fill=TRUE)
cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
cat('"org_code": "dc",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "dccas",', fill=TRUE)
cat('\t"data": ',json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)



sink()
close(newfile)

## VALIDATE JSON
test <- readLines("dccas.json")
validate(test)