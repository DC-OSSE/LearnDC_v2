source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)
library(dplyr)

er <- sqlQuery(dbrepcard_prod,"select * from equity_report_school_longitudinal where school_year = '2015-2016' and reported=1 and metric='Student Characteristics'") %>%
mutate(lea_code=sapply(lea_code,leadgr,4),school_code=sapply(school_code,leadgr,4),enrollment=ifelse(subgroup %in% 'All' & grade %in% 'All',nsize,round(school_score,3)),subgroup=ifelse(subgroup %in% c('Male','Female'),toupper(subgroup),subgroup),grade=ifelse(grade %in% c("All","PK3","PK4","KG","UN"),grade,paste0("grade ",grade))) %>%
select(school_code,year,subgroup,grade,enrollment)

strtable(er)

key_index <- 2:4
value_index <- 5
num_orphans <- 0


for(i in unique(er$school_code)){
	setwd(paste(root_dir, 'Export/JSON/school', sep=''))
	
	if (file.exists(i)){
	    setwd(file.path(i))
	} else {
	    dir.create(file.path(i))
	    setwd(file.path(i))
	    num_orphans <- num_orphans + 1

	}

	.tmp <- subset(er, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.school_name <- .tmp$school_name[1]

	newfile <- file(paste0("enrollment_equity.json"), encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	# cat('"org_name": "','",', sep="", fill=TRUE)
	cat('"org_code": "',i,'"',',', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "enrollment_equity",', fill=TRUE)
	cat('\t"data": ',.json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))
