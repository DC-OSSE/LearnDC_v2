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

state_move <- select(move_wide[which(move_wide$school_code=='0161'),],year,month,entry=state_entry,withdrawl=state_withdrawal,net_cumulative=state_net_cumulative)

strtable(state_move)

key_index <- 1:2
value_index <- 3:5

setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

	nested_list <- lapply(1:nrow(state_move), FUN = function(i){ 
                             list(key = list(state_move[i,key_index]), 
                             	val = list(state_move[i,value_index]))
                           })

	json <- toJSON(nested_list, na="null")
	json <- gsub("[[","",json, fixed=TRUE)
	json <- gsub("]]","",json, fixed=TRUE)
	json <- prettify(json)


	newfile <- file("mid_year_entry_and_withdrawal.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "state",', sep="", fill=TRUE)
	cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
	cat('"org_code": "dc",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "mid_year_entry_and_withdrawal",', fill=TRUE)
	cat('\t"data": ',json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)

	## VALIDATE JSON
	test <- readLines("mid_year_entry_and_withdrawal.json")
	validate(test)