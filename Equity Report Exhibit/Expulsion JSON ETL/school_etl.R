setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Expulsion JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

exp <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Metric], [NSize], [SchoolScore], [AverageScore]
		FROM [dbo].[equity_longitudinal] WHERE [Metric] in ('Expulsion Rate','Expulsions')")


exp_long <- melt(exp, id.vars = c("School_Year", "School_Code", "Student_Group", "Metric"))
exp_long$Metric<- paste0(exp_long$Metric, "_",exp_long$variable)
exp_long$variable <- NULL
exp_wide <- dcast(exp_long, School_Year + School_Code + Student_Group ~ Metric, value.var = "value")




colnames(exp_wide) <- c("year","school_code","subgroup","state_expulsion_rate","explusion_rate_n","expulsion_rate","state_expulsions","expulsions_n","expulsions")
exp_wide$explusion_rate_n <- NULL
exp_wide$expulsions_n <- NULL

exp_wide$expulsion_rate <- exp_wide$expulsion_rate/100
exp_wide$state_expulsion_rate <- exp_wide$state_expulsion_rate/100

exp_wide <- select(exp_wide, school_code, year,subgroup, expulsions, expulsion_rate, state_expulsions, state_expulsion_rate)

exp_wide$school_code <- sapply(exp_wide$school_code, leadgr, 4)


# setwd('U:/LearnDC ETL V2/Export/CSV/school')
# write.csv(exp_wide, "Equity_Report_Expulsion_School.csv", row.names=FALSE)


key_index <- c(2,3)
value_index <- c(4,5,6,7)



for(i in unique(exp_wide$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	if(file.exists(i)){
	    setwd(file.path(i))
	}	

	.tmp <- subset(exp_wide, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.school_name <- .tmp$school_name[1]


	newfile <- file("expulsions.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "expulsions",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}



