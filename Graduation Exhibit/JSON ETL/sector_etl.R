source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)

sector_grad <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[graduation_sector_exhibit] where reported = 1 and cohort_size >= 25 and isnull(graduates,'')!=''")

sector_grad <- subset(sector_grad, lea_code==0)
sector_grad$lea_code <- sapply(sector_grad$lea_code, leadgr, 4)
sector_grad$acgr <- round(sector_grad$graduates/sector_grad$cohort_size,3)
sector_grad$acgr <- ifelse(sector_grad$reported == 0 & sector_grad$reason_not_reported == 'n<25',NA,sector_grad$acgr)
sector_grad$suppressed <-ifelse(sector_grad$reported == 0 & sector_grad$reason_not_reported == 'n<25',1,0)
sector_grad$graduates <- NA
sector_grad$cohort_size <- NA


key_index <- c(3,4,5)
value_index <- c(10,11)
num_orphans <- 0


for(i in unique(sector_grad$lea_code)){
	setwd(paste(root_dir, 'Export/JSON/lea', sep=''))
	
	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}


	.tmp <- subset(sector_grad, lea_code == i)

	.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })

	.json <- toJSON(.nested_list, na="null")
	.json <- gsub("[[","",.json, fixed=TRUE)
	.json <- gsub("]]","",.json, fixed=TRUE)
	.json <- prettify(.json)


	.sector_name <- .tmp$lea_name[1]

	newfile <- file("graduation.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.sector_name),'",', sep="", fill=TRUE)
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