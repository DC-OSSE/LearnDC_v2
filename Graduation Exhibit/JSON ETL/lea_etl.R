source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)

lea_grad <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[graduation_lea_exhibit] where reported = 1 and cohort_size >= 25 and isnull(graduates,'')!=''")

lea_grad$lea_code <- sapply(lea_grad$lea_code, leadgr, 4)


key_index <- c(3,4,5)
value_index <- c(6,7)
num_orphans <- 0

school_dir <- sqlFetch(dbrepcard, 'schooldir_sy1314')
lea_dir <- unique(school_dir[c("lea_code","lea_name")])
lea_dir$lea_code <- sprintf("%04d", lea_dir$lea_code)
lea_dir <- subset(lea_dir, lea_code %in% lea_grad$lea_code)


for(i in unique(lea_dir$lea_code)){
	setwd(paste(root_dir, 'Export/JSON/lea', sep=''))
	
	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}


	.tmp <- subset(lea_grad, lea_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.lea_name <- .tmp$lea_name[1]

	newfile <- file("graduation.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.lea_name),'",', sep="", fill=TRUE)
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