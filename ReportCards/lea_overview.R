source("U:R/tomkit.R")

lea_dir <- sqlQuery(dbrepcard,'select distinct lea_code,lea_name from dbo.schooldir_sy1415_draft')
lea_dir$lea_code <- sapply(lea_dir$lea_code, leadgr, 4)
charters <- c('0000','Public Charter Schools')
lea_dir <- rbind(lea_dir,charters)

num_orphans <- 0

for(i in unique(lea_dir$lea_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/lea")

	if (file.exists(i)){
	    setwd(file.path(i))
	} else {
	    dir.create(file.path(i))
	    setwd(file.path(i))
		num_orphans <- num_orphans + 1
	}

	.tmp <- subset(lea_dir, lea_code == i)
	.lea_name <- .tmp$lea_name[1]

	newfile <- file("overview.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('"org_type": "lea",', sep="", fill=TRUE)
	cat('"org_name": "',gsub("\n", "",.lea_name),'",', sep="", fill=TRUE)
	cat('"org_code": "',i,'"',',', sep="", fill=TRUE)
	cat('"exhibit": {', fill=TRUE)
	cat('\t"id": "overview"', fill=TRUE)
		cat('\t}', fill=TRUE)
	cat('}', fill=TRUE)

	sink()
	close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))
