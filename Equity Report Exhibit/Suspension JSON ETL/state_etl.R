source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)
library(reshape2)
library(dplyr)

susp <- sqlQuery(dbrepcard_prod,"select * from equity_report_state_longitudinal where reported=1 and metric in ('Suspended 1+','Suspended 11+','Total Suspensions')") %>%
mutate(subgroup=ifelse(subgroup %in% c('Male','Female'),toupper(subgroup),subgroup)) %>% select(-starts_with("days"),-(school_year),-(grade),-starts_with("mo"),-starts_with("re"),-(count),-(nsize),-(enrollment))

susp_long <- melt(susp, id.vars = c("year","subgroup","metric","population"))
susp_long$metric<- paste0(susp_long$metric, "_",susp_long$variable)
susp_long$variable <- NULL
susp_wide <- dcast(susp_long, year + subgroup +population ~ metric, value.var = "value")
colnames(susp_wide) <- c("year","subgroup","population","suspended_1","suspended_11","incidents")
susp_wide$suspended_1 <- round(susp_wide$suspended_1,3)
susp_wide$suspended_11 <- round(susp_wide$suspended_11,4)
susp_wide$state_suspended_1 <- NA
susp_wide$state_suspended_11 <- NA
susp_wide$state_incidents <- NA
strtable(susp_wide)

key_index <- 1:3
value_index <- 4:9

	setwd(paste(root_dir, 'Export/JSON/state/DC', sep=''))

	nested_list <- lapply(1:nrow(susp_wide), FUN = function(i){ 
                             list(key = list(susp_wide[i,key_index]), 
                             	val = list(susp_wide[i,value_index]))
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