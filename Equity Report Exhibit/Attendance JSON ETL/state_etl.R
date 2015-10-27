setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Attendance JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

isa <- sqlQuery(dbrepcard_prod,"select * from equity_report_state_longitudinal where reported=1 and metric='In-Seat Attendance'") %>% 
	mutate(subgroup=ifelse(subgroup %in% c('Male','Female'),toupper(subgroup),ifelse(toupper(subgroup) %in% "ELL","LEP",subgroup)),population="All",state_in_seat_attendance=NA,average_daily_attendance=NA,state_average_daily_attendance=NA) %>% select(-starts_with("days"),-(school_year),-(grade),-starts_with("mo"),-starts_with("re"),-(count),-(nsize),-(enrollment),-(metric)) %>% rename(in_seat_attendance=score)
isa <- isa[c(1:2,4,3,5:7)]

isa$in_seat_attendance <- round(isa$in_seat_attendance,3)

strtable(isa)

key_index <- 1:3
value_index <- 4:7

	setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

	nested_list <- lapply(1:nrow(isa), FUN = function(i){ 
                             list(key = list(isa[i,key_index]), 
                             	val = list(isa[i,value_index]))
                           })

	json <- toJSON(nested_list, na="null")
	json <- gsub("[[","",json, fixed=TRUE)
	json <- gsub("]]","",json, fixed=TRUE)
	json <- prettify(json)


	newfile <- file("attendance.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "state",', sep="", fill=TRUE)
	cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
	cat('"org_code": "dc",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "attendance",', fill=TRUE)
	cat('\t"data": ',json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)

	## VALIDATE JSON
	test <- readLines("attendance.json")
	validate(test)