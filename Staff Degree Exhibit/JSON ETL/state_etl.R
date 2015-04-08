setwd("U:/LearnDC ETL V2/Staff Degree Exhibit/JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)

state_degree <- sqlQuery(dbrepcard_prod,"select * from dbo.staff_degree_state_exhibit")
# state_degree <- subset(state_degree, num_total >= 10)
colnames(state_degree)[2] <- "school_category"

strtable(state_degree)

key_index <- 1:2
value_index <- 3:8
num_orphans <- 0


	setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

	nested_list <- lapply(1:nrow(state_degree), FUN = function(i){ 
                             list(key = list(state_degree[i,key_index]), 
                             	val = list(state_degree[i,value_index]))
                           })

	json <- toJSON(nested_list, na="null")
	json <- gsub("[[","",json, fixed=TRUE)
	json <- gsub("]]","",json, fixed=TRUE)
	json <- prettify(json)


	newfile <- file("staff_degree.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "state",', sep="", fill=TRUE)
	cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
	cat('"org_code": "dc",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "staff_degree",', fill=TRUE)
	cat('\t"data": ',json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)

	## VALIDATE JSON
	test <- readLines("staff_degree.json")
	validate(test)