source(paste(root_dir,'imports/helpers.R',sep=''))

library(jsonlite)
library(plyr)

lea_sped <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[apr] WHERE lea != 'state'")

lea_sped$lea_code <- sapply(lea_sped$lea_code, leadgr, 4)
lea_sped$on_target <- ifelse(is.na(lea_sped$val), NA, lea_sped$on_target %in% c('1'))

key_index <- c(1,2)
value_index <- 5:10
num_orphans <- 0


for(i in unique(lea_sped$lea_code)){
	setwd(paste(root_dir, 'Export/JSON/lea', sep=''))

	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}


	.tmp <- subset(lea_sped, lea_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.lea_name <- .tmp$lea_name[1]

	newfile <- file("sped_apr.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.lea_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'"',',', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "sped_apr",', fill=TRUE)
	cat('\t"data": ',.json, fill=TRUE)
	cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))