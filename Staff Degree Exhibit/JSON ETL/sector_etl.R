setwd("U:/LearnDC ETL V2/Staff Degree Exhibit/JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)

sector_degree <- sqlQuery(dbrepcard_prod,"select * from dbo.staff_degree_sector_exhibit")
sector_degree$lea_code <- sapply(sector_degree$lea_code, leadgr, 4)
sector_degree <- subset(sector_degree, lea_code=='0000')
# sector_degree <- subset(sector_degree, num_total >= 10 & lea_code=='0000')

strtable(sector_degree)

key_index <- c(1,4)
value_index <- 5:10
num_orphans <- 0


for(i in unique(sector_degree$lea_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/lea")

	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}
	

	.tmp <- subset(sector_degree, lea_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)

	.lea_name <- .tmp$lea_name[1]


	newfile <- file("staff_degree.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.lea_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "staff_degree",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))