setwd("U:/LearnDC ETL V2/Enrollment Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)


school_enr <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[enrollment_school_exhibit]")

school_enr <- subset(school_enr, enrollment >= 10)


setwd('./Data')
write.csv(school_enr, "school_enrollment.csv", row.names=FALSE)


setwd("U:/LearnDC ETL V2/Enrollment Exhibit/JSON ETL/Data/Enrollment_school_lv_json")




key_index <- c(5,6,7)
value_index <- 8



for(i in unique(school_enr$school_code)){
	.tmp <- subset(school_enr, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.school_name <- .tmp$school_name[1]

	newfile <- file(paste0("Enrollment_School_",i,".JSON"), encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat('"org_code": ',i,',', sep="", fill=TRUE)
	cat('"exhibit_id": "enrollment",', fill=TRUE)
	cat('"data": ',.json, fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}


