source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)
library(reshape2)
library(dplyr)

disc <- sqlQuery(dbrepcard_prod,"select * from equity_report_school_longitudinal where school_year = '2015-2016' and reported=1 and metric in('Total Expulsions','Expulsion Rate','Suspended 1+','Suspended 11+','Total Suspensions')") %>% mutate(lea_code=sapply(lea_code,leadgr,4),school_code=sapply(school_code,leadgr,4),subgroup=ifelse(subgroup %in% c('Male','Female'),toupper(subgroup),subgroup)) %>% select(-(school_year),-(lea_code),-(school_name),-(lea_name),-(reported),-(reported),-(reason_not_reported),-(grade),-(enrollment),-(month))

expel <- disc %>% filter(metric %in% c('Total Expulsions','Expulsion Rate'))
exp_long <- melt(expel, id.vars = c("year","school_code","subgroup","metric"))
exp_long$metric <- paste0(exp_long$metric, "_",exp_long$variable)
exp_long$variable <- NULL
exp_wide <- dcast(exp_long,year + school_code + subgroup ~ metric,value.var="value")
colnames(exp_wide) <- c("year","school_code","subgroup","state_expulsion_rate","explusion_rate_n","expulsion_rate","state_expulsions","expulsions_n","expulsions")
exp_wide$explusion_rate_n <- NULL
exp_wide$expulsions_n <- NULL
exp_wide$expulsion_rate <- round(exp_wide$expulsion_rate, 4)
exp_wide$state_expulsion_rate <- round(exp_wide$state_expulsion_rate,4)
exp_wide$expulsions <- ifelse(is.na(exp_wide$expulsions), 0, exp_wide$expulsions)
exp_wide$state_expulsions <- ifelse(is.na(exp_wide$state_expulsions), 0, exp_wide$state_expulsions)

exp <- select(exp_wide,school_code,year,subgroup,expulsions,expulsion_rate,state_expulsions,state_expulsion_rate)

strtable(exp)

key_index <- 2:3
value_index <- 4:7
num_orphans <- 0


for(i in unique(exp$school_code)){
	setwd(paste(root_dir, 'Export/JSON/school', sep=''))
	
	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}
	

	.tmp <- subset(exp, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.school_name <- .tmp$school_name[1]


	newfile <- file("expulsions.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.school_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "expulsions",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))

