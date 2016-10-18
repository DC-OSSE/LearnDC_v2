source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)
library(reshape2)
library(dplyr)

myew <- sqlQuery(dbrepcard_prod,"select * from equity_report_state_longitudinal where school_year='2015-2016' and reported=1 and metric in ('Entry','Exit','Net Cumulative')") %>% mutate(month=ifelse(month %in% 'October',10,ifelse(month %in% 'November',11,ifelse(month %in% 'December',12,ifelse(month %in% 'January',1,ifelse(month %in% 'February',2,ifelse(month %in% 'March',3,ifelse(month %in% 'April',4,ifelse(month %in% 'May',5,NA))))))))) %>% arrange(month_order) %>% select(-starts_with("days"),-(school_year),-(grade),-(month_order),-starts_with("re"),-starts_with("days"),-(count),-(nsize),-(enrollment))

move_long <- melt(myew,id.vars=c("year","subgroup","month","metric","population"))
move_long$metric <- paste0(move_long$metric,"_",move_long$variable)
move_long$variable <- NULL
move_wide <- dcast(move_long,year + subgroup + month + population ~ metric,value.var="value")
colnames(move_wide) <- c("year","subgroup","month","population","entry","withdrawal","net_cumulative")
move_wide$withdrawal <- round(move_wide$withdrawal,3)
move_wide$entry <- round(move_wide$entry,3)
move_wide$net_cumulative <- round(move_wide$net_cumulative,3)
move_wide$state_entry <- NA
move_wide$state_withdrawal <- NA
move_wide$state_net_cumulative <- NA

mvmt <- select(move_wide,year,month,population,entry,withdrawal,net_cumulative,state_entry,state_withdrawal,state_net_cumulative)

strtable(mvmt)

key_index <- 1:3
value_index <- 4:9

	setwd(paste(root_dir, 'Export/JSON/state/DC', sep=''))

	nested_list <- lapply(1:nrow(mvmt), FUN = function(i){ 
                             list(key = list(mvmt[i,key_index]), 
                             	val = list(mvmt[i,value_index]))
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