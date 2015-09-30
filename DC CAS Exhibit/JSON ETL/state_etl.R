setwd("X:/Learn DC/State Equity Report Development")
source("U:/R/tomkit.R")
library(jsonlite)

state_cas <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[cas_state_exhibit]")
state_cas <- subset(state_cas, (enrollment_status == "full_year" & n_test_takers >= 25) | (enrollment_status == "all" & n_test_takers >= 10))
state_cas$population <- "All"

state_cas_gen <- subset(state_cas,grade %notin% c('grade AO','UN'))
state_cas_gen$population <- "Gen"
state_cas_gen$n_eligible <- round(state_cas_gen$n_eligible * .75,0)
state_cas_gen$n_test_takers <- round(state_cas_gen$n_test_takers * .75,0)
state_cas_gen$proficient_or_advanced <- round(state_cas_gen$proficient_or_advanced * .75,0)
state_cas_gen$below_basic <- round(state_cas_gen$below_basic * .75,0)
state_cas_gen$basic <- round(state_cas_gen$basic * .75,0)
state_cas_gen$proficient <- round(state_cas_gen$proficient * .75,0)
state_cas_gen$advanced <- round(state_cas_gen$advanced * .75,0)

state_cas_alt <- subset(state_cas,grade %in% c('grade AO','UN','All'))
# state_cas_alt$population <- "Alt"
state_cas_alt$n_eligible <- round(state_cas_alt$n_eligible * .25,0)
state_cas_alt$n_test_takers <- round(state_cas_alt$n_test_takers * .25,0)
state_cas_alt$proficient_or_advanced <- round(state_cas_alt$proficient_or_advanced * .25,0)
state_cas_alt$below_basic <- round(state_cas_alt$below_basic * .25,0)
state_cas_alt$basic <- round(state_cas_alt$basic * .25,0)
state_cas_alt$proficient <- round(state_cas_alt$proficient * .25,0)
state_cas_alt$advanced <- round(state_cas_alt$advanced * .25,0)

state_cas_all <- rbind.data.frame(state_cas,state_cas_gen,state_cas_alt)

strtable(state_cas_all)

setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

key_index <- c(1:5,13)
value_index <- 6:12

nested_list <- lapply(1:nrow(state_cas_all), FUN = function(i){ 
  list(key = list(state_cas_all[i,key_index]), 
       val = list(state_cas_all[i,value_index]))
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