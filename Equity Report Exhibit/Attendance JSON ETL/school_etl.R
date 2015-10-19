setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Attendance JSON ETL")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

isa <- sqlQuery(dbrepcard_prod,"select * from equity_report_school_longitudinal where metric in ('In-Seat Attendance Rate') and reported=1") %>% mutate(lea_code=sapply(lea_code,leadgr,4),school_code=sapply(school_code,leadgr,4),subgroup=ifelse(subgroup %in% c('Male','Female'),toupper(subgroup),subgroup)) %>% select(school_code,year,subgroup,metric,school_score,average_score,nsize)

att_long <- melt(isa,id.vars=c("year","school_code","subgroup","metric"))
att_long$metric<- paste0(att_long$metric,"_",att_long$variable)
att_long$variable <- NULL

attw <- dcast(att_long,year + school_code + subgroup ~ metric,value.var="value")
colnames(attw) <- c("year","school_code","subgroup","state_in_seat_attendance","in_seat_attendance_n","in_seat_attendance")
attw$in_seat_attendance_n <- NULL
attw <- select(attw, school_code, year,subgroup, in_seat_attendance, state_in_seat_attendance)
attw$in_seat_attendance <- round(attw$in_seat_attendance,3)
attw$state_in_seat_attendance <- round(attw$state_in_seat_attendance/100,3)
attw$average_daily_attendance <- NA
attw$state_average_daily_attendance <- NA

strtable(attw)

key_index <- c(2,3)
value_index <- c(4,5,6,7)
num_orphans <- 0


for(i in unique(attw$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}	

	.tmp <- subset(attw, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)



	.school_name <- .tmp$school_name[1]


	newfile <- file("attendance.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.school_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "attendance",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))

