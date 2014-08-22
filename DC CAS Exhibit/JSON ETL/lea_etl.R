setwd("U:/LearnDC ETL V2/DC CAS Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)

lea_cas <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[cas_lea_exhibit]")


lea_cas <- subset(lea_cas, (enrollment_status == "full_year" & n_test_takers >= 25) | (enrollment_status == "all" & n_test_takers >= 10))

setwd('./Data')
write.csv(lea_cas, "DCCAS_lea_lv.csv", row.names=FALSE)


setwd('U:/LearnDC ETL V2/DC CAS Exhibit/JSON ETL/Data/DCCAS_lea_lv_JSON')


key_index <- c(1,4:7)
value_index <- 8:14


for(i in unique(lea_cas$lea_code)){
	.tmp <- subset(lea_cas, lea_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.lea_name <- .tmp$lea_name[1]

	newfile <- file(paste0("DCCAS_LEA_",i,".JSON"), encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',.lea_name,'",', sep="", fill=TRUE)
	cat('"org_code": ',i,',', sep="", fill=TRUE)
	cat('"exhibit_id": "dccas",', fill=TRUE)
	cat('"data": ',.json, fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}


