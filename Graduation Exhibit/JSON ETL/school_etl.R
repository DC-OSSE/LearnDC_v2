setwd("U:/LearnDC ETL V2/Graduation Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)


school_grad <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[graduation_school_exhibit]")

school_grad <- subset(school_grad, cohort_size >= 10)


setwd('./data')
write.csv(school_grad, "Graduation_School.csv", row.names=FALSE)


key_index <- c(3,4)
value_index <- c(5,6)


setwd('U:/LearnDC ETL V2/Graduation Exhibit/JSON ETL/data/Graduation_School_lv_JSON')


for(i in unique(school_grad$school_code)){
	.tmp <- subset(school_grad, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.lea_name <- .tmp$lea_name[1]
	.school_name <- .tmp$school_name[1]


	newfile <- file(paste0("Graduation_School_",i,".JSON"), encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat('"org_code": ',i,',', sep="", fill=TRUE)
	cat('"exhibit_id": "graduation",', fill=TRUE)
	cat('"data": ',.json, fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}

