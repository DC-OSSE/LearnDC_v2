source(paste(root_dir,'imports/helpers.R',sep=''))

lea_dir <- sqlQuery(dbrepcard_prod,'select distinct lea_code, lea_name, school_year from dbo.lea_overview_vw')
lea_dir$lea_code <- sapply(lea_dir$lea_code, leadgr, 4)
charters <- c('0000','Public Charter Schools')
lea_dir <- rbind(lea_dir,charters)

num_orphans <- 0

for(i in unique(lea_dir$lea_code)){
	setwd(paste(root_dir, 'Export/JSON/lea', sep=''))

	.tmp <- subset(lea_dir, lea_code == i)
	.lea_name <- .tmp$lea_name[1]

	if(!file.exists(i) && .tmp$school_year[1] == '2016-2017'){
		dir.create(file.path(i))
	}
	if(file.exists(i)){
		setwd(file.path(i))
	}

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
