setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Mid Year Entry and Withdrawal JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

move <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Month],[Metric], [NSize], [SchoolScore], [AverageScore] FROM [dbo].[equity_longitudinal] WHERE [Metric] in ('Entry','Withdrawal','Net Cumulative')")


move_long <- melt(move, id.vars = c("School_Year", "School_Code", "Student_Group","Month", "Metric"))
move_long$Metric <- paste0(move_long$Metric, "_",move_long$variable)
move_long$variable <- NULL
move_wide <- dcast(move_long, School_Year + School_Code + Student_Group + Month ~ Metric, value.var = "value")


colnames(move_wide) <- c("year","school_code","subgroup","month","state_entry","entry_n","entry","state_net_cumulative","net_cumulative_n","net_cumulative","state_withdrawal","withdrawal_n","withdrawal")
move_wide$entry_n <- NULL
move_wide$net_cumulative_n <- NULL
move_wide$withdrawal_n <- NULL

move_wide$withdrawal <- round(move_wide$withdrawal,2)





move_wide <- select(move_wide, school_code, year, month, entry, withdrawal, net_cumulative, state_entry, state_withdrawal, state_net_cumulative)

move_wide$school_code <- sapply(move_wide$school_code, leadgr, 4)


# setwd('U:/LearnDC ETL V2/Export/CSV/school')
# write.csv(move_wide, "Equity_Report_Mid_Year_Entry_and_Withdrawal_School.csv", row.names=FALSE)


key_index <- c(2,3)
value_index <- c(4:9)


for(i in unique(move_wide$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	if(file.exists(i)){
	    setwd(file.path(i))
	}	

	.tmp <- subset(move_wide, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)

	.school_name <- .tmp$school_name[1]


	newfile <- file("mid_year_entry_and_withdrawal.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "mid_year_entry_and_withdrawal",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}



