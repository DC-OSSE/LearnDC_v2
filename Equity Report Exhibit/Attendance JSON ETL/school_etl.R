setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Attendance JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

att <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Metric], [NSize], [SchoolScore], [AverageScore]
		FROM [dbo].[equity_longitudinal] WHERE [Metric] in ('In-Seat Attendance Rate')")


att_long <- melt(att, id.vars = c("School_Year", "School_Code", "Student_Group", "Metric"))
att_long$Metric<- paste0(att_long$Metric, "_",att_long$variable)
att_long$variable <- NULL
att_wide <- dcast(att_long, School_Year + School_Code + Student_Group ~ Metric, value.var = "value")




colnames(att_wide) <- c("year","school_code","subgroup","state_in_seat_attendance","in_seat_attendance_n","in_seat_attendance")
att_wide$in_seat_attendance_n <- NULL


att_wide$in_seat_attendance <- att_wide$in_seat_attendance/100
att_wide$state_in_seat_attendance <- att_wide$state_in_seat_attendance/100






att_wide <- select(att_wide, school_code, year,subgroup, in_seat_attendance, state_in_seat_attendance)
att_wide$average_daily_attendance <- 'null'
att_wide$state_average_daily_attendance <- 'null'


att_wide$school_code <- sapply(att_wide$school_code, leadgr, 4)


# setwd('U:/LearnDC ETL V2/Export/CSV/school')
# write.csv(att_wide, "Equity_Report_Attendance_School.csv", row.names=FALSE)



key_index <- c(2,3)
value_index <- c(4,5,6,7)


for(i in unique(att_wide$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	if(file.exists(i)){
	    setwd(file.path(i))
	}	

	.tmp <- subset(att_wide, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list)
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- gsub('"null"','null',.json, fixed=TRUE)


	.school_name <- .tmp$school_name[1]


	newfile <- file("attendance.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "attendance",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}



