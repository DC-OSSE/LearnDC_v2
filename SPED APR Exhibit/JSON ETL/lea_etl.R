setwd("U:/LearnDC ETL V2/DC CAS Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)


lea_sped <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[SPED_APR_Indicators_1314]")


setwd('U:/LearnDC ETL V2/Export/CSV/lea')
write.csv(lea_sped, "SPED_APR_LEA.csv", row.names=FALSE)


lea_sped$lea_code <- sapply(lea_sped$lea_code, leadgr, 4)


key_index <- 1
value_index <- 3:14


colnames(lea_sped) <- gsub("indicator_","", colnames(lea_sped))

for(i in unique(lea_sped$lea_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/lea")

	if (file.exists(i)){
	    setwd(file.path(i))
	}

	.tmp <- subset(lea_sped, lea_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.lea_name <- .tmp$lea_name[1]

	newfile <- file("sped_apr.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',.lea_name,'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'"',',', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "sped_apr",', fill=TRUE)
	cat('\t"data": ',.json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}

