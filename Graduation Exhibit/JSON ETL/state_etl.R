source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)

state_grad <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[graduation_state_exhibit] where reported = 1 and population='all' and cohort_size >= 25 and isnull(graduates,'')!=''")
state_grad$acgr <- round(state_grad$graduates/state_grad$cohort_size,3)
state_grad$acgr <- ifelse(state_grad$reported == 0 & state_grad$reason_not_reported == 'n<25',NA,state_grad$acgr)
state_grad$suppressed <-ifelse(state_grad$reported == 0 & state_grad$reason_not_reported == 'n<25',1,0)
state_grad$graduates <- NA
state_grad$cohort_size <- NA

key_index <- c(1:4)
value_index <- c(9,10)


nested_list <- lapply(1:nrow(state_grad), FUN = function(i){ 
  list(key = list(state_grad[i,key_index]), 
       val = list(state_grad[i,value_index]))
})

json <- toJSON(nested_list, na="null")
json <- gsub("[[","",json, fixed=TRUE)
json <- gsub("]]","",json, fixed=TRUE)
json <- prettify(json)

setwd(paste(root_dir, 'Export/JSON/state/DC', sep=''))

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