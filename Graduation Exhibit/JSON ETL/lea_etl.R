setwd("U:/LearnDC ETL V2/Graduation Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)


lea_grad <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[graduation_lea_exhibit_w2014]")
lea_grad <- subset(lea_grad, cohort_size >= 25)


setwd('U:/LearnDC ETL V2/Export/CSV/lea')
write.csv(lea_grad, "Graduation_LEA.csv", row.names=FALSE)


lea_grad$lea_code <- sapply(lea_grad$lea_code, leadgr, 4)


key_index <- c(3,4)
value_index <- c(5,6)

school_dir <- sqlFetch(dbrepcard, 'schooldir_sy1314')
lea_dir <- unique(school_dir[c("lea_code","lea_name")])
lea_dir$lea_code <- sprintf("%04d", lea_dir$lea_code)
lea_dir <- subset(lea_dir, lea_code %in% lea_grad$lea_code)


for(i in unique(lea_dir$lea_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/lea")

	if (file.exists(i)){
	    setwd(file.path(i))
	}

	.tmp <- subset(lea_grad, lea_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.lea_name <- .tmp$lea_name[1]

	newfile <- file("graduation.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',.lea_name,'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "graduation",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}
