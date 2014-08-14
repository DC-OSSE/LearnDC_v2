setwd("U:/LearnDC ETL V2/DC CAS Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)

state_cas <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[State_cas_exhibit]")

state_cas <- subset(state_cas, (enrollment_status == "full_year" & n_test_takers >= 25) | (enrollment_status == "all" & n_test_takers >= 10))
 
setwd('./Data/')

write.csv(state_cas, "DCCAS_state_lv.csv", row.names=FALSE)


key_index <- 1:5
value_index <- 6:12

nested_list <- lapply(1:nrow(state_cas), FUN = function(i){ 
                             list(key = list(state_cas[i,key_index]), 
                             	val = list(state_cas[i,value_index]))
                           })


json <- toJSON(nested_list)
json <- gsub("[[","",json, fixed=TRUE)
json <- gsub("]]","",json, fixed=TRUE)



## write to file

org_type <- "state"
org_code <- "STATE"

state_version <- sqlQuery(dbrepcard, "SELECT TOP 1 
        [version_number],
        [timestamp]
    FROM [dbo].[ver_control_statefile]
    ORDER BY [version_number] DESC")

next_version <- state_version$version_number + 0.1

newfile <- file("DCCAS_state_lv.JSON", encoding="UTF-8")



sink(newfile)
cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "state",', sep="", fill=TRUE)
cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
cat('"org_code": "dc",', sep="", fill=TRUE)
cat('"exhibit_id": "dccas",', fill=TRUE)
cat('"data": ',json, fill=TRUE)
cat('}', fill=TRUE)

sink()
close(newfile)

## VALIDATE JSON
test <- readLines("DCCAS_state_lv.JSON")
validate(test)