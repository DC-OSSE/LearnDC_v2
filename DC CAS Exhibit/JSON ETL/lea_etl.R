setwd("U:/LearnDC ETL V2/DC CAS Exhibit/JSON ETL")

source("U:/R/tomkit.R")
library(jsonlite)

lea_cas <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[cas_lea_exhibit]")


lea_cas <- subset(lea_cas, (enrollment_status == "full_year" & n_test_takers >= 25) | (enrollment_status == "all" & n_test_takers >= 10))

setwd('U:/LearnDC ETL V2/Export/CSV/lea')
write.csv(lea_cas, "DCCAS_LEA.csv", row.names=FALSE)


lea_cas$lea_code <- sapply(lea_cas$lea_code, leadgr, 4)


key_index <- c(1,4:7)
value_index <- 8:14
num_orphans <- 0


school_dir <- sqlFetch(dbrepcard, 'schooldir_sy1314')
lea_dir <- unique(school_dir[c("lea_code","lea_name")])
lea_dir$lea_code <- sprintf("%04d", lea_dir$lea_code)
lea_dir <- subset(lea_dir, lea_code %in% lea_cas$lea_code)



for(i in unique(lea_dir$lea_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/lea")

	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}

	.tmp <- subset(lea_cas, lea_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.lea_name <- .tmp$lea_name[1]

	newfile <- file("dccas.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.lea_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'"',',', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "dccas",', fill=TRUE)
	cat('\t"data": ',.json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))
