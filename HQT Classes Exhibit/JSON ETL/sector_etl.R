setwd("U:/LearnDC ETL V2/HQT Classes Exhibit/JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)

# school_hqt$lea_code[which(school_hqt$school_code=='0480')] <- '4001'
# school_hqt$lea_name[which(school_hqt$school_code=='0480')] <- 'State-Level Reporting LEA'
sector_hqt <- sqlQuery(dbrepcard_prod,"select * from dbo.hqt_classes_sector_exhibit_bk")
sector_hqt$lea_code <- sapply(sector_hqt$lea_code, leadgr, 4)
sector_hqt <- subset(sector_hqt, lea_code=='0000')
# sector_hqt <- subset(sector_hqt, num_total_classes >= 10 & lea_code=='0000')

# setwd('U:/LearnDC ETL V2/Export/CSV/school')
# write.csv(school_hqt, "HQT_School.csv", row.names=FALSE)

strtable(sector_hqt)

key_index <- 3:4
value_index <- 5:7

for(i in unique(sector_hqt$lea_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/lea")

	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}
	

	tmp <- subset(sector_hqt, lea_code == i)

	nested_list <- lapply(1:nrow(tmp), FUN = function(i){ 
                             list(key = list(tmp[i,key_index]), 
                             	val = list(tmp[i,value_index]))
                           })

	json <- toJSON(nested_list, na="null")
	json <- gsub("[[","",json, fixed=TRUE)
	json <- gsub("]]","",json, fixed=TRUE)
	json <- prettify(json)

	lea_name <- tmp$lea_name[1]


	newfile <- file("hqt_classes.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",lea_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "hqt_classes",', fill=TRUE)
	cat('\t"data": ', json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}

test <- readLines("hqt_classes.json")
validate(test)