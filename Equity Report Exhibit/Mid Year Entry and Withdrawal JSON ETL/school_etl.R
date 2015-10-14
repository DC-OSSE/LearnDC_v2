setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Mid Year Entry and Withdrawal JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

move <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Month],[Metric], [NSize], [SchoolScore], [AverageScore] FROM [dbo].[equity_longitudinal3] WHERE [Metric] in ('Entry','Withdrawal','Net Cumulative')")
move <- subset(move, move$School_Code!=1119 & move$School_Code!=216 & move$School_Code!=104 & move$School_Code !=137 & move$School_Code!=168 & move$School_Code!=128 & move$School_Code!=462 & move$School_Code!=456)
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

##
myew <- sqlQuery(dbrepcard_prod,"select * from equity_report_school_longitudinal where reported=1 and metric in('Withdrawal','Entry','Net Cumulative')") %>% mutate(school_code=sapply(school_code,leadgr,4),month=ifelse(month %in% 'October',10,ifelse(month %in% 'November',11,ifelse(month %in% 'December',12,ifelse(month %in% 'January',1,ifelse(month %in% 'February',2,ifelse(month %in% 'March',3,ifelse(month %in% 'April',4,ifelse(month %in% 'May',5,NA)))))))),year=ifelse(month %in% 10:12,2014,2015)) %>% select(-(starts_with("lea")),-(starts_with("re")),-(school_year),-(enrollment),-(grade),-(school_name),-(nsize))
move_long <- melt(myew,id.vars=c("year","school_code","subgroup","month","metric"))
move_long$metric <- paste0(move_long$metric,"_",move_long$variable)
move_long$variable <- NULL
move_wide <- dcast(move_long,year + school_code + subgroup + month ~ metric,value.var="value")
colnames(move_wide) <- c("year","school_code","subgroup","month","state_entry","entry","state_net_cumulative","net_cumulative","state_withdrawal","withdrawal")
move_wide$withdrawal <- round(move_wide$withdrawal,2)
move_wide$state_withdrawal <- round(move_wide$state_withdrawal,2)
move_wide$state_entry <- round(move_wide$state_entry,2)
move_wide$entry <- round(move_wide$entry,2)
move_wide$net_cumulative <- round(move_wide$net_cumulative,2)
move_wide$state_net_cumulative <- round(move_wide$state_net_cumulative,2)
mvmt <- select(move_wide,school_code,year,month,entry,withdrawal,net_cumulative,state_entry,state_withdrawal,state_net_cumulative)

strtable(mvmt)

key_index <- 2:3
value_index <- 4:9
num_orphans <- 0


for(i in unique(mvmt$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")
	
	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}
	

	.tmp <- subset(mvmt, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.school_name <- .tmp$school_name[1]


	newfile <- file("mid_year_entry_and_withdrawal.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.school_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "mid_year_entry_and_withdrawal",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))

