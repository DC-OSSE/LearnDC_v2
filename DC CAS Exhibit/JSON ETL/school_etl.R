setwd("U:/LearnDC ETL V2/DC CAS Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)

school_cas <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[cas_school_exhibit]")
school_cas$rownames <- NULL


school_cas <- subset(school_cas, (enrollment_status == "full_year" & n_test_takers >= 25) | (enrollment_status == "all" & n_test_takers >= 10))

setwd('U:/LearnDC ETL V2/Export/CSV/school')
write.csv(school_cas, "DCCAS_School.csv", row.names=FALSE)


key_index <- c(1,6:9)
value_index <- 10:15

school_cas$school_code <- sapply(school_cas$school_code, leadgr, 4)


for(i in unique(school_cas$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	if (file.exists(i)){
	    setwd(file.path(i))
	} 

	.tmp <- subset(school_cas, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.school_name <- .tmp$school_name[1]

	newfile <- file("dccas.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'"',',', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "dccas",', fill=TRUE)
	cat('\t"data": ',.json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}


