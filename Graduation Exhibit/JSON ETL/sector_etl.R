setwd("U:/LearnDC ETL V2/Graduation Exhibit/JSON ETL")
source("U:/R/RODBC_Connections.R")
source("U:/R/tomkit.R")
library(jsonlite)


sector_grad <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[graduation_sector_exhibit_w2014]")
sector_grad <- subset(sector_grad, cohort_size >= 10 & !is.na(graduates))


# setwd('U:/LearnDC ETL V2/Export/CSV/lea')
# write.csv(lea_grad, "Graduation_LEA.csv", row.names=FALSE)

sector_grad <- subset(sector_grad, lea_code==0)
sector_grad$lea_code <- sapply(sector_grad$lea_code, leadgr, 4)


key_index <- c(3,4,5)
value_index <- c(6,7)
num_orphans <- 0


for(i in unique(sector_grad$lea_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/lea")
	
	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}


	.tmp <- subset(sector_grad, lea_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.sector_name <- .tmp$lea_name[1]

	newfile <- file("graduation.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.sector_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "graduation",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))