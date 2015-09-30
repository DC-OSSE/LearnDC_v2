setwd("U:/LearnDC ETL V2/Enrollment Exhibit/JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)


state_enr <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[enrollment_state_exhibit_pcsb_alterations]")
state_enr$population <- "All"
state_enr[which(state_enr$subgroup=='All' & state_enr$grade=='All'),]$enrollment <- sum(subset(state_enr,subgroup=='All')$enrollment)-1

state_enr_gen <- subset(state_enr,grade %notin% c('grade AO','UN'))
state_enr_gen$population <- "Gen"
state_enr_gen$enrollment <- state_enr_gen$enrollment * .75
state_enr_gen[which(state_enr_gen$enrollment>1),]$enrollment <- round(state_enr_gen[which(state_enr_gen$enrollment>1),]$enrollment,0)
state_enr_gen[which(state_enr_gen$enrollment<=1),]$enrollment <- state_enr[which(state_enr$enrollment<=1),]$enrollment

state_enr_alt <- subset(state_enr,grade %in% c('grade AO','UN','All'))
state_enr_alt$population <- "Alt"
state_enr_alt$enrollment <- state_enr_alt$enrollment * .25
state_enr_alt[which(state_enr_alt$enrollment>1),]$enrollment <- round(state_enr_alt[which(state_enr_alt$enrollment>1),]$enrollment,0)
state_enr_alt[which(state_enr_alt$enrollment<=1),]$enrollment <- state_enr[which(state_enr$enrollment<=1),]$enrollment

state_enr_all <- rbind.data.frame(state_enr,state_enr_gen,state_enr_alt)

strtable(state_enr_all)

setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

key_index <- c(1:3,5)
value_index <- 4

nested_list <- lapply(1:nrow(state_enr_all), FUN = function(i){ 
  list(key = list(state_enr_all[i,key_index]), 
       val = list(state_enr_all[i,value_index]))
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