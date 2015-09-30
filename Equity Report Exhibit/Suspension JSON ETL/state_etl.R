setwd("X:/Learn DC/State Equity Report Development")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

susp <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Metric], [NSize], [SchoolScore], [AverageScore]
  	FROM [dbo].[equity_longitudinal3] WHERE [Metric] in ('Suspended 1+','Suspended 11+','Total Suspensions')")
susp <- subset(susp, !is.na(School_Code))

susp_long <- melt(susp, id.vars = c("School_Year", "School_Code", "Student_Group", "Metric"))
susp_long$Metric<- paste0(susp_long$Metric, "_",susp_long$variable)
susp_long$variable <- NULL

susp_wide <- dcast(susp_long, School_Year + School_Code + Student_Group ~ Metric, value.var = "value")
colnames(susp_wide) <- c("year","school_code","subgroup","state_suspended_1","suspended_1_n","suspended_1","state_suspended_11","suspended_11_n","suspended_11","state_incidents","incidents_n","incidents")
susp_wide$suspended_11_n <- NULL
susp_wide$suspended_1_n <- NULL
susp_wide$incidents_n <- NULL
susp_wide$state_incidents <- NULL
susp_wide$state_suspended_1 <- susp_wide$state_suspended_1/100
susp_wide$suspended_1 <- susp_wide$suspended_1/100
susp_wide$state_suspended_11 <- susp_wide$state_suspended_11/100
susp_wide$suspended_11 <- susp_wide$suspended_11/100
susp_wide <- select(susp_wide, school_code, year,subgroup, suspended_1, suspended_11, state_suspended_1, state_suspended_11, incidents)
susp_wide$school_code <- sapply(susp_wide$school_code, leadgr, 4)

state_vals <- susp_wide[which(susp_wide$school_code=='0161'),]
state_vals$suspended_1 <- state_vals$state_suspended_1
state_vals$suspended_11 <- state_vals$state_suspended_11
state_vals$incidents <- state_vals$incidents

state_susp_all <- state_vals %>%
  mutate(population='All',state_suspended_1=NA,state_suspended_11=NA,state_incidents=NA) %>%
  select(year,subgroup,population,suspended_1,suspended_11,incidents,
  	state_suspended_1,state_suspended_11,state_incidents)

state_susp_gen <- state_vals %>%
  mutate(population='Gen',suspended_1=round(suspended_1/1.25,4),
         suspended_11=round(suspended_11/1.25,4),incidents=round(incidents/1.25,0),
         state_suspended_1=NA,state_suspended_11=NA,state_incidents=NA) %>%
  select(year,subgroup,population,suspended_1,suspended_11,incidents,
  	state_suspended_1,state_suspended_11,state_incidents)

state_susp_alt <- state_vals %>%
  mutate(population='Alt',suspended_1=round(suspended_1/1.75,4),
         suspended_11=round(suspended_11/1.75,4),incidents=round(incidents/1.75,0),
         state_suspended_1=NA,state_suspended_11=NA,state_incidents=NA) %>%
  select(year,subgroup,population,suspended_1,suspended_11,incidents,
  	state_suspended_1,state_suspended_11,state_incidents)

state_susp <- rbind.data.frame(state_susp_all,state_susp_gen,state_susp_alt)

strtable(state_susp)

key_index <- 1:3
value_index <- 4:9

	setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

	nested_list <- lapply(1:nrow(state_susp), FUN = function(i){ 
                             list(key = list(state_susp[i,key_index]), 
                             	val = list(state_susp[i,value_index]))
                           })

	json <- toJSON(nested_list, na="null")
	json <- gsub("[[","",json, fixed=TRUE)
	json <- gsub("]]","",json, fixed=TRUE)
	json <- prettify(json)


	newfile <- file("suspensions.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "state",', sep="", fill=TRUE)
	cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
	cat('"org_code": "dc",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "suspensions",', fill=TRUE)
	cat('\t"data": ',json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)

	## VALIDATE JSON
	test <- readLines("suspensions.json")
	validate(test)