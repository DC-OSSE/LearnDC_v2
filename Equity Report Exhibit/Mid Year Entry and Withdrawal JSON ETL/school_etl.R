source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)
library(reshape2)
library(dplyr)

##
myew <- sqlQuery(dbrepcard_prod,"select * from equity_report_school_longitudinal where school_year='2015-2016' and reported=1 and metric in('Withdrawal','Entry','Net Cumulative')") %>% mutate(school_code=sapply(school_code,leadgr,4),month=ifelse(month %in% 'October',10,ifelse(month %in% 'November',11,ifelse(month %in% 'December',12,ifelse(month %in% 'January',1,ifelse(month %in% 'February',2,ifelse(month %in% 'March',3,ifelse(month %in% 'April',4,ifelse(month %in% 'May',5,NA))))))))) %>% select(-(starts_with("lea")),-(starts_with("re")),-(school_year),-(enrollment),-(grade),-(school_name),-(nsize))
move_long <- melt(myew,id.vars=c("year","school_code","subgroup","month","metric"))
move_long$metric <- paste0(move_long$metric,"_",move_long$variable)
move_long$variable <- NULL
move_wide <- dcast(move_long,year + school_code + subgroup + month ~ metric,value.var="value")
colnames(move_wide) <- c("year","school_code","subgroup","month","state_entry","entry","state_net_cumulative","net_cumulative","state_withdrawal","withdrawal")
move_wide$withdrawal <- round(move_wide$withdrawal,3)
move_wide$state_withdrawal <- round(move_wide$state_withdrawal,3)
move_wide$state_entry <- round(move_wide$state_entry,3)
move_wide$entry <- round(move_wide$entry,3)
move_wide$net_cumulative <- round(move_wide$net_cumulative,3)
move_wide$state_net_cumulative <- round(move_wide$state_net_cumulative,3)
mvmt <- select(move_wide,school_code,year,month,entry,withdrawal,net_cumulative,state_entry,state_withdrawal,state_net_cumulative)

strtable(mvmt)

key_index <- 2:3
value_index <- 4:9
num_orphans <- 0


for(i in unique(mvmt$school_code)){
	setwd(paste(root_dir, 'Export/JSON/school', sep=''))
	
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

