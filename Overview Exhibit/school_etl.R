source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)

schools <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[schools_overview]")
schools$school_code <- sapply(schools$school_code, leadgr, 4)

for(i in unique(schools$school_code)){
	setwd(paste(root_dir, 'Export/JSON/school', sep=''))
	
	.tmp <- subset(schools, school_code == i)

	.lea_name <- .tmp$lea_name[1]
	.school_name <- .tmp$school_name[1]

	if(!file.exists(i) && .tmp$school_year[1] == '2016-2017'){
		dir.create(file.path(i))
	}
	if(file.exists(i)){
		setwd(file.path(i))
	}

	newfile <- file("overview.json", encoding="UTF-8")
	sink(newfile)

	cat('{', fill=TRUE)

	cat('\t"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat('\t"org_type": "school",', sep="", fill=TRUE)
	cat('\t"org_name": "',gsub("\n", "",.school_name),'",', sep="", fill=TRUE)
	cat('\t"org_code": "',i,'",', sep="", fill=TRUE)
	cat('\t"closed": ',jsonBoolean(.tmp$closed[1]),',', sep="", fill=TRUE)
	cat('\t"charter": ',jsonBoolean(.tmp$charter[1]),',', sep="", fill=TRUE)
	cat('\t"school_type": [\n\t\t',paste(shQuote(.tmp$school_type, type="cmd"), collapse=", "),'\n\t ],', sep="", fill=TRUE)
	cat('\t"great_schools": {\n\t\t"gs_url": ',checkna_str(.tmp$great_schools_url[1]),'\n\t},\n', sep="")
	cat('\t"ward": ',checkna_str(.tmp$ward[1]),',', sep="", fill=TRUE)
	cat('\t"grades_serviced": [',paste(shQuote(as.list((strsplit(.tmp$grades_serviced[1], ",")[[1]])), type="cmd"),collapse=","),'],', sep="", fill=TRUE)
	cat('\t"grades_accepted": ',checkna_str(.tmp$grades_accepted[1]),',', sep="", fill=TRUE)
	cat('\t"description": ',checkna_str(.tmp$description[1]),',\n', sep="")
	cat('\t"address": {', fill=TRUE)
	cat('\t\t"line_1": ',checkna_str(.tmp$address_line_1[1]),',', sep="", fill=TRUE)
	cat('\t\t"line_2": ',checkna_str(.tmp$address_line_2[1]),',', sep="", fill=TRUE)
	cat('\t\t"city": ',checkna_str(.tmp$address_city[1]),',', sep="", fill=TRUE)
	cat('\t\t"state": ',checkna_str(.tmp$address_state[1]),',', sep="", fill=TRUE)
	cat('\t\t"zip": ',checkna_str(.tmp$address_zip[1]),',', sep="", fill=TRUE)
	cat('\t\t"lat": ',checkna(.tmp$latitude[1]),',', sep="", fill=TRUE)
	cat('\t\t"long": ',checkna(.tmp$longitude[1]),'', sep="", fill=TRUE)
	cat('\t},', fill=TRUE)
	cat('\t"transit": ',checkna_str(.tmp$transit[1]),',\n', sep="")
	cat('\t"website": ',checkna_str(.tmp$website[1]),',', sep="", fill=TRUE)
	cat('\t"facebook": ',checkna_str(.tmp$facebook[1]),',', sep="", fill=TRUE)
	cat('\t"twitter": ',checkna_str(.tmp$twitter[1]),',', sep="", fill=TRUE)
	cat('\t"external_report_card": ',checkna_str(.tmp$external_report_card[1]),',', sep="", fill=TRUE)
	cat('\t"equity_report_url": \n',checkna_str(.tmp$equity_report_url[1]),',\n', sep="")
	cat('\t"contact": [', sep="")
	if(!is.na(.tmp$contact_name[1])){
		cat('{', fill=TRUE)
		cat('\t\t"name": ',checkna_str(.tmp$contact_name[1]),',', sep="", fill=TRUE)
		cat('\t\t"title": ',checkna_str(.tmp$contact_title[1]),',', sep="", fill=TRUE)
		cat('\t\t"phone": ',checkna_str(.tmp$contact_phone[1]),',', sep="", fill=TRUE)
		cat('\t\t"email": ',checkna_str(.tmp$contact_email[1]),'', sep="", fill=TRUE)
		cat('\t\t}', sep="")
	}
	cat(']', fill=TRUE)
	cat('}', fill=TRUE)


	sink()
	close(newfile)
}