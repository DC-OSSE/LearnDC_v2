setwd("U:/LearnDC ETL V2/Equity Report Exhibit/MGP JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

mgp_m <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Metric], [NSize], [SchoolScore]
	FROM [dbo].[equity_longitudinal] WHERE [Metric] in ('CAS Math Growth','CAS Math 2-year Growth')")

mgp_r <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Metric], [NSize], [SchoolScore]
	FROM [dbo].[equity_longitudinal] WHERE [Metric] in ('CAS Reading Growth','CAS Reading 2-year Growth')")


mgp_m$subject <- "Math"
mgp_r$subject <- "Reading"
mgp <- rbind(mgp_m, mgp_r)
mgp <- subset(mgp, NSize >= 25)


mgp$Metric[which(mgp$Metric %in% c("CAS Math Growth","CAS Reading Growth"))] <- "mgp_1yr"
mgp$Metric[which(mgp$Metric %in% c("CAS Math 2-year Growth","CAS Reading 2-year Growth"))] <- "mgp_2yr"

mgp_wide <- dcast(mgp, School_Code + School_Year + Student_Group + subject ~ Metric, value.var = "SchoolScore" )


colnames(mgp_wide) <- c("school_code","year","subgroup","subject","mgp_1yr","mgp_2yr")
mgp_wide$school_code <- sapply(mgp_wide$school_code, leadgr, 4)


# setwd('U:/LearnDC ETL V2/Export/CSV/school')
# write.csv(susp_wide, "Equity_Report_MGP_School.csv", row.names=FALSE)


key_index <- c(2,3,4)
value_index <- c(5,6)


for(i in unique(mgp_wide$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	if(file.exists(i)){
	    setwd(file.path(i))
	}	

	.tmp <- subset(mgp_wide, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.school_name <- .tmp$school_name[1]


	newfile <- file("mgp_scores.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "mgp_scores",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}



