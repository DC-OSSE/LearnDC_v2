setwd("U:/LearnDC ETL V2/Graduation Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)


school_grad <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[graduation_school_exhibit]")

school_grad <- subset(school_grad, cohort_size >= 10)


setwd('U:/LearnDC ETL V2/Export/CSV/school')
write.csv(school_grad, "Graduation_School.csv", row.names=FALSE)


school_grad$school_code <- sapply(school_grad$school_code, leadgr, 4)

key_index <- c(5,6)
value_index <- c(7,8)


for(i in unique(school_grad$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	if(file.exists(i)){
	    setwd(file.path(i))
	}	

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


	newfile <- file("graduation.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "graduation",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}

