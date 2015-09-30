setwd("X:/Learn DC/State Equity Report Development")
source("U:/R/tomkit.R")
library(jsonlite)


state_grad <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[graduation_state_exhibit_w2014]")
state_grad <- subset(state_grad, cohort_size >= 10 & !is.na(graduates))


state_grad$population <- "All"

state_grad_gen <- state_grad
state_grad_gen$population <- "Gen"
state_grad_gen$graduates <- round(state_grad_gen$graduates * .75,0)
state_grad_gen$cohort_size <- round(state_grad_gen$cohort_size * .75,0)

state_grad_alt <- state_grad
state_grad_alt$population <- "Alt"
state_grad_alt$graduates <- round(state_grad_gen$graduates * .25,0)
state_grad_alt$cohort_size <- round(state_grad_gen$cohort_size * .25,0)

state_grad_all <- rbind.data.frame(state_grad,state_grad_gen,state_grad_alt)

strtable(state_grad_all)

key_index <- c(1:3,6)
value_index <- c(4,5)


nested_list <- lapply(1:nrow(state_grad_all), FUN = function(i){ 
  list(key = list(state_grad_all[i,key_index]), 
       val = list(state_grad_all[i,value_index]))
})

json <- toJSON(nested_list, na="null")
json <- gsub("[[","",json, fixed=TRUE)
json <- gsub("]]","",json, fixed=TRUE)
json <- prettify(json)

setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

newfile <- file("graduation.json", encoding="UTF-8")
sink(newfile)

cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "state",', sep="", fill=TRUE)
cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
cat('"org_code": "dc",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "graduation",', fill=TRUE)
cat('\t"data": ',json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)



sink()
close(newfile)

## VALIDATE JSON
test <- readLines("graduation.json")
validate(test)