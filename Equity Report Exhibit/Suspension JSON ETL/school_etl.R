source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)
library(reshape2)
library(dplyr)

disc <- sqlQuery(dbrepcard_prod,"select * from equity_report_school_longitudinal where school_year = '2015-2016' and reported=1 and metric in('Total Expulsions','Expulsion Rate','Suspended 1+','Suspended 11+','Total Suspensions')") %>% mutate(lea_code=sapply(lea_code,leadgr,4),school_code=sapply(school_code,leadgr,4),subgroup=ifelse(subgroup %in% c('Male','Female'),toupper(subgroup),subgroup)) %>% select(-(school_year),-(lea_code),-(school_name),-(lea_name),-(reported),-(reported),-(reason_not_reported),-(grade),-(enrollment),-(month))
susp <- disc %>% filter(metric %in% c('Suspended 1+','Suspended 11+','Total Suspensions'))
susp_long <- melt(susp, id.vars = c("year","school_code","subgroup","metric"))
susp_long$metric<- paste0(susp_long$metric, "_",susp_long$variable)
susp_long$variable <- NULL
suspw <- dcast(susp_long, year + school_code + subgroup ~ metric, value.var = "value")
colnames(suspw) <- c("year","school_code","subgroup","state_suspended_1","suspended_1_n","suspended_1","state_suspended_11","suspended_11_n","suspended_11","state_incidents","incidents_n","incidents")
suspw$suspended_11_n <- NULL
suspw$suspended_1_n <- NULL
suspw$incidents_n <- NULL
suspw$state_incidents <- NULL
suspw$suspended_1 <- round(suspw$suspended_1,3)
suspw$suspended_11 <- round(suspw$suspended_11,4)
suspw$state_suspended_1 <- round(suspw$state_suspended_1,3)
suspw$state_suspended_11 <- round(suspw$state_suspended_11,4)
suspw$incidents <- ifelse(is.na(suspw$incidents), 0, suspw$incidents)
suspend <- select(suspw,school_code,year,subgroup,suspended_1,suspended_11,state_suspended_1,state_suspended_11,incidents)

strtable(suspend)

key_index <- 2:3
value_index <- 4:8
num_orphans <- 0

for(i in unique(suspend$school_code)){
	setwd(paste(root_dir, 'Export/JSON/school', sep=''))

	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}
	

	.tmp <- subset(suspend, suspend == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.school_name <- .tmp$school_name[1]


	newfile <- file("suspensions.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.school_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "suspensions",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))

