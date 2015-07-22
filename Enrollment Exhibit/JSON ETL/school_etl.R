setwd("U:/LearnDC ETL V2/Enrollment Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)


school_enr <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[enrollment_school_exhibit_w2015]")

school_enr <- subset(school_enr, enrollment >= 10)
school_enr <- subset(school_enr, lea_code %notin% c(4001,4002))



school_enr$school_code <- sapply(school_enr$school_code, leadgr, 4)


key_index <- c(5,6,7)
value_index <- 8
num_orphans <- 0


for(i in unique(school_enr$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")
	
	if (file.exists(i)){
	    setwd(file.path(i))
	} else {
	    dir.create(file.path(i))
	    setwd(file.path(i))
	    num_orphans <- num_orphans + 1
	}

	.tmp <- subset(school_enr, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.school_name <- .tmp$school_name[1]

	newfile <- file(paste0("enrollment.json"), encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	# cat('"org_name": "','",', sep="", fill=TRUE)
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
