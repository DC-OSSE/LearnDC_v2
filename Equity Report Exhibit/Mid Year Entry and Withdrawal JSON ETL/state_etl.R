setwd("X:/Learn DC/State Equity Report Development")
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

state_vals <- move_wide[which(move_wide$school_code=='0161'),]
state_vals$entry <- state_vals$state_entry
state_vals$withdrawal <- state_vals$state_withdrawal
state_vals$net_cumulative <- state_vals$state_net_cumulative

state_move_all <- state_vals %>%
  mutate(population='All',state_entry=NA,state_withdrawal=NA,state_net_cumulative=NA) %>%
  select(year,month,population,entry,withdrawal,net_cumulative,
  	state_entry,state_withdrawal,state_net_cumulative)

state_move_gen <- state_vals %>%
  mutate(population='Gen',entry=round(entry/1.25,4),withdrawal=round(withdrawal/1.25,4),
  	net_cumulative=round(net_cumulative/1.25,4),
         state_entry=NA,state_withdrawal=NA,state_net_cumulative=NA) %>%
  select(year,month,population,entry,withdrawal,net_cumulative,
  	state_entry,state_withdrawal,state_net_cumulative)

state_move_alt <- state_vals %>%
  mutate(population='Alt',entry=round(entry/1.75,4),withdrawal=round(withdrawal/1.75,4),
         net_cumulative=round(net_cumulative/1.75,4),
         state_entry=NA,state_withdrawal=NA,state_net_cumulative=NA) %>%
  select(year,month,population,entry,withdrawal,net_cumulative,
  	state_entry,state_withdrawal,state_net_cumulative)

state_move <- rbind.data.frame(state_move_all,state_move_gen,state_move_alt)

strtable(state_move)

key_index <- 1:3
value_index <- 4:9

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