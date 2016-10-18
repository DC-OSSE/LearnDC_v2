source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)
library(dplyr)

enr <- sqlQuery(dbrepcard_prod,"select * from equity_report_state_longitudinal where school_year = '2015-2016' and reported=1 and metric='Student Characteristics'") %>%
mutate(enrollment=ifelse(subgroup %in% 'All' & grade %in% 'All',nsize,round(score,3)),subgroup=ifelse(subgroup %in% c('Male','Female'),toupper(subgroup),subgroup),grade=ifelse(substr(grade,0,1) %in% 0,substr(grade,2,2),grade),grade=ifelse(grade %in% c("All","PK3","PK4","KG","UN"),grade,paste0("grade ",grade))) %>% filter(subgroup %notin% 'AtRisk' & reported==1) %>% select(year,grade,subgroup,population,enrollment)

strtable(enr)

setwd(paste(root_dir, 'Export/JSON/state/DC', sep=''))

key_index <- 1:4
value_index <- 5

nested_list <- lapply(1:nrow(enr), FUN = function(i){ 
  list(key = list(enr[i,key_index]), 
       val = list(enr[i,value_index]))
})

json <- toJSON(nested_list, na="null")
json <- gsub("[[","",json, fixed=TRUE)
json <- gsub("]]","",json, fixed=TRUE)
json <- prettify(json)

newfile <- file("enrollment_equity.json", encoding="UTF-8")
sink(newfile)

cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "state",', sep="", fill=TRUE)
cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
cat('"org_code": "dc",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "enrollment_equity",', fill=TRUE)
cat('\t"data": ',json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)

sink()
close(newfile)

## VALIDATE JSON
test <- readLines("enrollment_equity.json")
validate(test)