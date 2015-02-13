setwd("U:/LearnDC ETL V2/HQT Exhibit/JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(plyr)

# school_hqt$lea_code[which(school_hqt$school_code=='0480')] <- '4001'
# school_hqt$lea_name[which(school_hqt$school_code=='0480')] <- 'State-Level Reporting LEA'

state_hqt <- sqlQuery(dbrepcard_prod,"select * from dbo.hqt_classes_state_exhibit_w2014")
state_hqt <- subset(state_hqt, num_total_classes >= 10)


# setwd('U:/LearnDC ETL V2/Export/CSV/school')
# write.csv(school_hqt, "HQT_School.csv", row.names=FALSE)


key_index <- c(1,2)
value_index <- c(3,4,5)

	setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

	nested_list <- lapply(1:nrow(state_hqt), FUN = function(i){ 
                             list(key = list(state_hqt[i,key_index]), 
                             	val = list(state_hqt[i,value_index]))
                           })

	json <- toJSON(nested_list, na="null")
	json <- gsub("[[","",json, fixed=TRUE)
	json <- gsub("]]","",json, fixed=TRUE)
	json <- prettify(json)


	newfile <- file("hqt_classes.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "state",', sep="", fill=TRUE)
	cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
	cat('"org_code": "dc",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "hqt",', fill=TRUE)
	cat('\t"data": ',json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)

	## VALIDATE JSON
	test <- readLines("graduation.json")
	validate(test)