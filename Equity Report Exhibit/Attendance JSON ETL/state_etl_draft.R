setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Attendance JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

att <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Metric], [NSize], [SchoolScore], [AverageScore]
		FROM [dbo].[equity_longitudinal3] WHERE [Metric] in ('In-Seat Attendance Rate')")

att_long <- melt(att, id.vars = c("School_Year", "School_Code", "Student_Group", "Metric"))
att_long$Metric<- paste0(att_long$Metric, "_",att_long$variable)
att_long$variable <- NULL
att_wide <- dcast(att_long, School_Year + School_Code + Student_Group ~ Metric, value.var = "value")

colnames(att_wide) <- c("year","school_code","subgroup","state_in_seat_attendance","in_seat_attendance_n","in_seat_attendance")
att_wide$in_seat_attendance_n <- NULL
att_wide$in_seat_attendance <- att_wide$in_seat_attendance/100
att_wide$state_in_seat_attendance <- att_wide$state_in_seat_attendance/100
att_wide <- select(att_wide, school_code, year,subgroup, in_seat_attendance, state_in_seat_attendance)
att_wide$average_daily_attendance <- NA
att_wide$state_average_daily_attendance <- NA
att_wide$school_code <- sapply(att_wide$school_code, leadgr, 4)

state_att <- select(att_wide[which(att_wide$school_code=='0161'),],year,subgroup,in_seat_attendance=state_in_seat_attendance,average_daily_attendance=state_average_daily_attendance)

strtable(state_att)

key_index <- 1:2
value_index <- 3:4

	setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

	nested_list <- lapply(1:nrow(state_att), FUN = function(i){ 
                             list(key = list(state_att[i,key_index]), 
                             	val = list(state_att[i,value_index]))
                           })

	json <- toJSON(nested_list, na="null")
	json <- gsub("[[","",json, fixed=TRUE)
	json <- gsub("]]","",json, fixed=TRUE)
	json <- prettify(json)


	newfile <- file("attendance.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "state",', sep="", fill=TRUE)
	cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
	cat('"org_code": "dc",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "attendance",', fill=TRUE)
	cat('\t"data": ',json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)

	## VALIDATE JSON
	test <- readLines("attendance.json")
	validate(test)