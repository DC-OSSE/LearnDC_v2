source("U:/R/tomkit.R")
library(dplyr)
library(jsonlite)

state_acc <- sqlFetch(dbrepcard,"accountability_state")
state_acc$school_year <- '2013-2014'
state_acc$year <- 2014
state_acc <- select(state_acc,school_year,year,subgroup,read_size,read_score,math_size,math_score,comp_size,comp_score)

state_acc$subject <- NA
acc_read <- select(state_acc,year,subject,subgroup,read_score,read_size)
acc_read$subject <- "Reading"
acc_math <- select(state_acc,year,subject,subgroup,math_score,math_size)
acc_math$subject <- "Math"
acc_comp <- select(state_acc,year,subject,subgroup,comp_score,comp_size)
acc_comp$subject <- "Composition"


colnames(acc_comp) <- c("year","subject","subgroup","subject_score","subject_size")
colnames(acc_read) <- c("year","subject","subgroup","subject_score","subject_size")
colnames(acc_math) <- c("year","subject","subgroup","subject_score","subject_size")

acc_state <- rbind(acc_read,acc_math,acc_comp)

acc_state$subgroup[which(acc_state$subgroup=='African American')] <- 'BL7'
acc_state$subgroup[which(acc_state$subgroup=='Asian')] <- 'AS7'
acc_state$subgroup[which(acc_state$subgroup=='Economically Disadvantaged')] <- 'ECONOMY'
acc_state$subgroup[which(acc_state$subgroup=='English Learner')] <- 'LEP'
acc_state$subgroup[which(acc_state$subgroup=='Special Education')] <- 'SPED'
acc_state$subgroup[which(acc_state$subgroup=='Hispanic')] <- 'HI7'
acc_state$subgroup[which(acc_state$subgroup=='White')] <- 'WH7'
acc_state$subgroup[which(acc_state$subgroup=='Multiracial')] <- 'MU7'
acc_state$subgroup[which(acc_state$subgroup=='Male')] <- 'MALE'
acc_state$subgroup[which(acc_state$subgroup=='Female')] <- 'FEMALE'


setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

key_index <- 1:3
value_index <- 4:5


nested_list <- lapply(1:nrow(acc_state), FUN = function(i){ 
                             list(key = list(acc_state[i,key_index]), 
                             	val = list(acc_state[i,value_index]))
                           })


json <- toJSON(nested_list, na="null")
json <- gsub("[[","",json, fixed=TRUE)
json <- gsub("]]","",json, fixed=TRUE)
json <- prettify(json)

## write to file
newfile <- file("accountability.json", encoding="UTF-8")

sink(newfile)
cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "state",', sep="", fill=TRUE)
cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
cat('"org_code": "dc",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "accountability",', fill=TRUE)
cat('\t"data": ',json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)


sink()
close(newfile)

test <- readLines("accountability.json")
validate(test)