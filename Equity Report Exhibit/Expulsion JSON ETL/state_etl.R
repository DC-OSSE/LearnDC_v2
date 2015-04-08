setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Expulsion JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

exp <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Metric], [NSize], [SchoolScore], [AverageScore]
		FROM [dbo].[equity_longitudinal3] WHERE [Metric] in ('Expulsion Rate','Expulsions')")

exp_long <- melt(exp, id.vars = c("School_Year", "School_Code", "Student_Group", "Metric"))
exp_long$Metric <- paste0(exp_long$Metric, "_",exp_long$variable)
exp_long$Metric <- gsub(" ","_",exp_long$Metric)
exp_long$variable <- NULL

exp_wide <- dcast(exp_long, School_Year + School_Code + Student_Group ~ Metric, value.var = "value")
colnames(exp_wide) <- c("year","school_code","subgroup","state_expulsion_rate","explusion_rate_n","expulsion_rate","state_expulsions","expulsions_n","expulsions")
exp_wide$explusion_rate_n <- NULL
exp_wide$expulsions_n <- NULL
exp_wide$state_expulsion_rate <- exp_wide$state_expulsion_rate/100
exp_wide$expulsion_rate <- exp_wide$expulsion_rate/100
exp_wide <- select(exp_wide, school_code, year,subgroup, expulsions, expulsion_rate, state_expulsions, state_expulsion_rate)
exp_wide$school_code <- sapply(exp_wide$school_code, leadgr, 4)

state_exp <- select(exp_wide[which(exp_wide$school_code=='0161'),],year,subgroup,expulsions=state_expulsions,expulsion_rates=state_expulsion_rate)

strtable(state_exp)

key_index <- 1:2
value_index <- 3:4

setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

	nested_list <- lapply(1:nrow(state_exp), FUN = function(i){ 
                             list(key = list(state_exp[i,key_index]), 
                             	val = list(state_exp[i,value_index]))
                           })

	json <- toJSON(nested_list, na="null")
	json <- gsub("[[","",json, fixed=TRUE)
	json <- gsub("]]","",json, fixed=TRUE)
	json <- prettify(json)


	newfile <- file("expulsions.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "state",', sep="", fill=TRUE)
	cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
	cat('"org_code": "dc",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "expulsions",', fill=TRUE)
	cat('\t"data": ',json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)

	## VALIDATE JSON
	test <- readLines("expulsions.json")
	validate(test)