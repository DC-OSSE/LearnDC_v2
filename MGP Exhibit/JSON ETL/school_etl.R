setwd("U:/LearnDC ETL V2/MGP Exhibit/JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)


mgp <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[mgp_longitudinal]")
mgp <- subset(mgp, subgroup %notin% c("Not-LEP","Not-Economy","Not-SPED"))
# setwd('U:/LearnDC ETL V2/Export/CSV/school')
# write.csv(susp_wide, "Equity_Report_MGP_School.csv", row.names=FALSE)

mgp <- subset(mgp, group_fay_size >= 10)

mgp$school_code <- sapply(mgp$school_code, leadgr, 4)

key_index <- c(1,3,4)
value_index <- c(6,7)

for(i in unique(mgp$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	if(file.exists(i)){
	    setwd(file.path(i))
	}	

	.tmp <- subset(mgp, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.school_name <- .tmp$school_name[1]


	newfile <- file("mgp_scores.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "mgp_scores",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}



