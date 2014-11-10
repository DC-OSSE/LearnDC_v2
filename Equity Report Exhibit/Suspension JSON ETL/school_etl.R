setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Suspension JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

susp <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Metric], [NSize], [SchoolScore], [AverageScore]
		FROM [dbo].[equity_longitudinal] WHERE [Metric] in ('Suspended 1+','Suspended 11+')")


susp_long <- melt(susp, id.vars = c("School_Year", "School_Code", "Student_Group", "Metric"))
susp_long$Metric<- paste0(susp_long$Metric, "_",susp_long$variable)
susp_long$variable <- NULL
susp_wide <- dcast(susp_long, School_Year + School_Code + Student_Group ~ Metric, value.var = "value")




colnames(susp_wide) <- c("year","school_code","subgroup","state_suspended_1","suspended_1_n","suspended_1","state_suspended_11","suspended_11_n","suspended_11")
susp_wide$suspended_11_n <- NULL
susp_wide$suspended_1_n <- NULL

susp_wide$state_suspended_1 <- susp_wide$state_suspended_1/100
susp_wide$suspended_1 <- susp_wide$suspended_1/100
susp_wide$state_suspended_11 <- susp_wide$state_suspended_11/100
susp_wide$suspended_11 <- susp_wide$suspended_11/100


susp_wide <- select(susp_wide, school_code, year,subgroup, suspended_1, suspended_11, state_suspended_1, state_suspended_11)

susp_wide$school_code <- sapply(susp_wide$school_code, leadgr, 4)


# setwd('U:/LearnDC ETL V2/Export/CSV/school')
# write.csv(susp_wide, "Equity_Report_Suspension_School.csv", row.names=FALSE)




key_index <- c(2,3)
value_index <- c(4,5,6,7)


for(i in unique(susp_wide$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	if(file.exists(i)){
	    setwd(file.path(i))
	}	

	.tmp <- subset(susp_wide, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.school_name <- .tmp$school_name[1]


	newfile <- file("suspensions.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "suspensions",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}



