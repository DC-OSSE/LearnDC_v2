setwd("U:/LearnDC ETL V2/Enrollment Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)


lea_enr <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[enrollment_lea_exhibit]")

lea_enr <- subset(lea_enr, enrollment >= 10)


setwd('U:/LearnDC ETL V2/Export/CSV/lea')
write.csv(lea_enr, "Enrollment_LEA.csv", row.names=FALSE)


lea_enr$lea_code <- sapply(lea_enr$lea_code, leadgr, 4)


key_index <- c(3,4,5)
value_index <- 6


for(i in unique(lea_enr$lea_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/lea")

	if (file.exists(i)){
	    setwd(file.path(i))
	} else {
	    dir.create(file.path(i))
	    setwd(file.path(i))
	}

	.tmp <- subset(lea_enr, lea_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.lea_name <- .tmp$lea_name[1]

	newfile <- file("enrollment.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',.lea_name,'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'"',',', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "enrollment",', fill=TRUE)
	cat('\t"data": ',.json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}