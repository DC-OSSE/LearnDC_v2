setwd("U:/LearnDC ETL V2/Enrollment Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)


sector_enr <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[enrollment_sector_exhibit_w2015]")

sector_enr <- subset(sector_enr, enrollment >= 10)


sector_enr$lea_code <- sapply(sector_enr$lea_code, leadgr, 4)
sector_enr <- subset(sector_enr, lea_code=='0000')

key_index <- c(3,4,5)
value_index <- 6
num_orphans <- 0


for(i in unique(sector_enr$lea_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/lea")

	if (file.exists(i)){
	    setwd(file.path(i))
	} else {
	    dir.create(file.path(i))
	    setwd(file.path(i))
		num_orphans <- num_orphans + 1
	}

	.tmp <- subset(sector_enr, lea_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.lea_name <- .tmp$lea_name[1]

	newfile <- file("enrollment.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.lea_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'"',',', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "enrollment",', fill=TRUE)
	cat('\t"data": ',.json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))
