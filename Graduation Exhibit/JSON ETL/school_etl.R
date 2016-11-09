source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)

school_grad <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[graduation_school_exhibit] where (reported = 1 and cohort_size >= 25 and isnull(graduates,'')!='') or (isnull(graduates,'')!='' and subgroup = 'All')")
school_grad$school_code <- sapply(school_grad$school_code, leadgr, 4)
school_grad$acgr <- round(school_grad$graduates/school_grad$cohort_size,3)
school_grad$acgr <- ifelse(school_grad$reported == 0 & school_grad$reason_not_reported == 'n<25','n<25',school_grad$acgr)
school_grad$graduates <- NA
school_grad$cohort_size <- NA

key_index <- c(5,6,7)
value_index <- c(8,9,12)
num_orphans <- 0

for(i in unique(school_grad$school_code)){
	setwd(paste(root_dir, 'Export/JSON/school', sep=''))

	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}
	

	.tmp <- subset(school_grad, school_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)

	.lea_name <- .tmp$lea_name[1]
	.school_name <- .tmp$school_name[1]


	newfile <- file("graduation.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "school",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.school_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'",', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "graduation",', fill=TRUE)
	cat('\t"data": ', .json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))