setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Absences JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

abs <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Metric], [NSize], [SchoolScore], [AverageScore]
		FROM [dbo].[equity_longitudinal3] WHERE [Metric] in ('Unexcused Absences 1-5','Unexcused Absences 6-10','Unexcused Absences 11-15','Unexcused Absences 16-25','Unexcused Absences > 25')")


abs_long <- melt(abs, id.vars = c("School_Year", "School_Code", "Student_Group", "Metric"))
abs_long$Metric<- paste0(abs_long$Metric, "_",abs_long$variable)
abs_long$variable <- NULL
abs_wide <- dcast(abs_long, School_Year + School_Code + Student_Group ~ Metric, value.var = "value")

colnames(abs_wide) <- c("year","school_code","subgroup","state_more_than_25_days","more_than_25_days_n","more_than_25_days","state_1_5_days","X1_5_days_n","1-5_days","state_11-15_days","X11_15_days_n","11-15_days","state_16-25_days","X16_25_days_n","16-25_days","state_6-10_days","X6_10_days_n","6-10_days")
abs_wide$X1_5_days_n <- NULL
abs_wide$X6_10_days_n <- NULL
abs_wide$X11_15_days_n <- NULL
abs_wide$X16_25_days_n <- NULL
abs_wide$more_than_25_days_n <- NULL


abs_wide <- abs_wide[,c(2,1,3,7,13,9,11,5,6,12,8,10,4)]

abs_wide$school_code <- sapply(abs_wide$school_code, leadgr, 4)


# setwd('U:/LearnDC ETL V2/Export/CSV/school')
# write.csv(abs_wide, "Equity_Report_Unexcused_Absences_School.csv", row.names=FALSE)

strtable(abs_wide)

key_index <- c(2,3)
value_index <- c(4:13)
num_orphans <- 0


for(i in unique(abs_wide$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	
	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}


	.tmp <- subset(abs_wide, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.school_name <- .tmp$school_name[1]


	newfile <- file("unexcused_absences.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.school_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "unexcused_absences",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))

