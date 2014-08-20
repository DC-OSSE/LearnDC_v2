setwd("U:/LearnDC ETL V2/DC CAS Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)

school_cas <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[school_cas_exhibit]")



school_cas <- subset(school_cas, (enrollment_status == "full_year" & n_test_takers >= 25) | (enrollment_status == "all" & n_test_takers >= 10))

setwd('./Data/DCCAS_school_lv_csv')
write.csv(school_cas, "DCCAS_school_lv.csv", row.names=FALSE)


setwd('U:/LearnDC ETL V2/DC CAS Exhibit/JSON ETL/Data/DCCAS_school_lv_JSON')


key_index <- c(2,5:8)
value_index <- 9:15


for(i in unique(school_cas$school_code)){
	.tmp <- subset(school_cas, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.school_name <- .tmp$school_name[1]

	newfile <- file(paste0("DCCAS_School_",i,".JSON"), encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat('"org_code": ',i,',', sep="", fill=TRUE)
	cat('"exhibit_id": "dccas",', fill=TRUE)
	cat('"data": ',.json, fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}


