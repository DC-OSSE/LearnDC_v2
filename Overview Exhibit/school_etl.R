source(paste(root_dir,'imports/helpers.R',sep=''))
source(paste(root_dir,'Overview Exhibit/school_functions.R', sep=''))
source(paste(root_dir,'Overview Exhibit/generalized.R', sep=''))

school_dir <- sqlFetch(dbrepcard,'schooldir_linked_sy1415')

school_dir$twitter <- NA ## Placeholder
school_dir$external <- NA ## Placeholder

school_dir$school_code <- sapply(school_dir$school_code, leadgr, 4)
school_dir$lea_code <- sapply(school_dir$lea_code, leadgr, 4)
school_dir <- subset(school_dir, school_code %notin% c("7000", "0948", "0958", "0480", "0472", "0465"))
school_dir <- subset(school_dir, lea_code != 6000)

## Unreadable Special Characters and Paragraph Spacings
school_dir$description <- gsub('"', "'", school_dir$description)
school_dir$description <- gsub('â€œ', "'", school_dir$description)
school_dir$description <- gsub('â€', "'", school_dir$description)
school_dir$description <- gsub('\n', "", school_dir$description)
school_dir$description <- gsub('\r', "", school_dir$description)
school_dir$website <- gsub('\n', "", school_dir$website)
school_dir$website <- gsub('\r', "", school_dir$website)
school_dir$profile_name <- gsub('\n', "", school_dir$profile_name)
school_dir$profile_name <- gsub('\r', "", school_dir$profile_name)

InsertPeople <- function(org_code, level){
	.lv <- level
	
	## MATH/READING
	.qry <- "SELECT * FROM [dbo].[people_sy1314]
		WHERE [school_code] = '" %+% org_code %+% "';"
	.dat_peep <- sqlQuery(dbrepcard, .qry)
	##print(.dat_peep)
	
	.ret <- c()
	if(!is.null(.dat_peep) & nrow(.dat_peep)>0){
		for(i in 1:nrow(.dat_peep)){
			.add <- indent(.lv) %+% '{\n'
			
			up(.lv)
			.add <- .add %+% paste(indent(.lv), '"name": ', checkna_str(.dat_peep$contact_name[i]),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"title": ', checkna_str(.dat_peep$role[i]),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"phone": ', checkna_str(.dat_peep$contact_phone[i]),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"email": ', checkna_str(.dat_peep$contact_email[i]), '\n', sep="")
			
			down(.lv)
			.add <- .add %+% paste(indent(.lv), '}', sep="")
				
			.ret <- c(.ret, .add)		
		}
	}
	return(paste(.ret, collapse=',\n'))
}

GetGrades <- function(org_code){
	## MATH/READING
	.qry <- sprintf("SELECT DISTINCT [grade] FROM [dbo].[current_roster_grades]
		WHERE [school_code] = '%s';",org_code)
	.sgrades <- sqlQuery(dbrepcard, .qry)

	.sgrades_df <- as.data.frame(apply(.sgrades, 1, leadgr, 2))
	colnames(.sgrades_df) <- "grade"

	.ret <- c()
	if(nrow(.sgrades_df)>0){
		for(i in 1:nrow(.sgrades_df)){
			.ret <- c(.ret, '"' %+%.sgrades_df$grade[i] %+% '"')
		}
	}
	return(paste(.ret, collapse=','))
}

# GetGrades <- function(org_code){
# 	## MATH/READING
# 	.qry <- sprintf("SELECT distinct t.grade
#   FROM reportcard_dev.[dbo].[enrollment_w2015_pkcbo] t
#   JOIN ( SELECT MAX(ea_year) AS max_year
#            FROM reportcard_dev.[dbo].[enrollment_w2015_pkcbo] mx
#        ) m
#     ON m.max_year = t.ea_year
# 		WHERE [school_code] = '%s';",ifelse(org_code!='0099',gsub("(^|[^0-9])0+", "\\1", org_code, perl = TRUE),'099'))
# 	.sgrades <- sqlQuery(dbrepcard, .qry)

# 	.sgrades_df <- as.data.frame(apply(.sgrades, 1, leadgr, 2))
# 	colnames(.sgrades_df) <- "grade"

# 	.ret <- c()
# 	if(nrow(.sgrades_df)>0){
# 		for(i in 1:nrow(.sgrades_df)){
# 			.ret <- c(.ret, '"' %+%.sgrades_df$grade[i] %+% '"')
# 		}
# 	}
# 	return(paste(.ret, collapse=','))
# }

WriteProfile <- function(org_code){
	.qry_profile <- sprintf("SELECT * FROM [dbo].[profile_urls]
		WHERE [school_code] = '%s'",org_code )
	.prog_profile <- sqlQuery(dbrepcard, .qry_profile)
	
	if(nrow(.prog_profile) == 0){
		return(NA)
	} 
	
	x <- .prog_profile$url[1]
	
	return(x)
}

GSUrl <- function(org_code){
	.qry_gsurl <- "SELECT * FROM [dbo].[gs_url]
		WHERE [school_code] = '" %+% org_code %+% "'"
	.gs_dat <- sqlQuery(dbrepcard, .qry_gsurl)
	
	if(nrow(.gs_dat)>0){
		return(trimall(.gs_dat$gs_url[1]))
	} else{
		return(NA)
	}
}


EquityUrl <- function(org_code){
	.qry_equity <- "SELECT * FROM [dbo].[equity_report_url_mapping]
		WHERE [school_code] = '" %+% org_code %+% "' AND [url_check] = 1 
		AND school_year = (SELECT MAX(school_year) FROM reportcard_dev.[dbo].[equity_report_url_mapping])"
	.equity_dat <- sqlQuery(dbrepcard, .qry_equity)
	if(nrow(.equity_dat )>0){
		return(trimall(.equity_dat$equity_url[1]))
	} else{
		return(NA)
	}
}

# GradesAccepted <- function(org_code){
# 	.qry_grades <- "SELECT [grade] FROM [dbo].[grades_accepted_sy1516]
# 		WHERE [school_code] = '" %+% org_code %+% "' AND [accept_application] = 1"
# 	.grades_dat <- sqlQuery(dbrepcard, .qry_grades)

# 	if(nrow(.grades_dat) > 0){
# 		.grades_dat_df <- as.data.frame(apply(.grades_dat, 1, leadgr, 2))
# 		colnames(.grades_dat_df) <- "grade"

# 		.ret <- c()
# 		if(nrow(.grades_dat_df)>0){
# 			for(i in 1:nrow(.grades_dat_df)){
# 				.ret <- c(.ret, '"' %+%.grades_dat_df$grade[i] %+% '"')
# 			}
# 		}
		
# 	} else {
# 		.qry <- sprintf("SELECT distinct t.grade
#   FROM reportcard_dev.[dbo].[enrollment_w2015_pkcbo] t
#   JOIN ( SELECT MAX(ea_year) AS max_year
#            FROM reportcard_dev.[dbo].[enrollment_w2015_pkcbo] mx
#        ) m
#     ON m.max_year = t.ea_year
# 		WHERE [school_code] = '%s';",ifelse(org_code!='0099',gsub("(^|[^0-9])0+", "\\1", org_code, perl = TRUE),'099'))
# 		.sgrades2 <- sqlQuery(dbrepcard, .qry)

# 		.sgrades_df2 <- as.data.frame(apply(.sgrades2, 1, leadgr, 2))
# 		colnames(.sgrades_df2) <- "grade"

# 		.ret <- c()
# 		if(nrow(.sgrades_df2)>0){
# 			for(i in 1:nrow(.sgrades_df2)){
# 			.ret <- c(.ret, '"' %+%.sgrades_df2$grade[i] %+% '"')
# 			}
# 		}

# 	}
# 	return(paste(.ret, collapse=','))
# }

GradesAccepted <- function(org_code){
	.qry_grades <- "SELECT [grade] FROM [dbo].[grades_accepted_sy1516]
		WHERE [school_code] = '" %+% org_code %+% "' AND [accept_application] = 1"
	.grades_dat <- sqlQuery(dbrepcard, .qry_grades)

	if(nrow(.grades_dat) > 0){
		.grades_dat_df <- as.data.frame(apply(.grades_dat, 1, leadgr, 2))
		colnames(.grades_dat_df) <- "grade"

		.ret <- c()
		if(nrow(.grades_dat_df)>0){
			for(i in 1:nrow(.grades_dat_df)){
				.ret <- c(.ret, '"' %+%.grades_dat_df$grade[i] %+% '"')
			}
		}
		
	} else {
		.qry <- sprintf("SELECT DISTINCT [grade] FROM [dbo].[current_roster_grades]
		WHERE [school_code] = '%s';",org_code)
	.sgrades <- sqlQuery(dbrepcard, .qry)

	.sgrades_df <- as.data.frame(apply(.sgrades, 1, leadgr, 2))
	colnames(.sgrades_df) <- "grade"

	.ret <- c()
	if(nrow(.sgrades_df)>0){
		for(i in 1:nrow(.sgrades_df)){
			.ret <- c(.ret, '"' %+%.sgrades_df$grade[i] %+% '"')
			}
		}
	}
	return(paste(.ret, collapse=','))
}



JSONTrueFalse <- function(x){
	
	x <- toupper(x)

	if(is.na(x) | is.null(x)){
		return('null')
	} else if(x %in% c('YES', 'TRUE', '1')){
		return('true')
	} else if(x %in% c('NO', 'FALSE', '0')){
		return('false')
	} else{
		return('null')
	}
}


## Yay for-loops!!!!!
num_orphans <- 0

for(i in unique(school_dir$school_code)){
	
	setwd(paste(root_dir,'Export/JSON/school',sep=''))

	if (file.exists(i)){
	    setwd(file.path(i))
	} else {
	    dir.create(file.path(i))
	    setwd(file.path(i))
		num_orphans <- num_orphans + 1
	}
	
	
	.tmp <- subset(school_dir, school_code == i)

	.org_type <- "school"
	.org_code <- .tmp$school_code[1]
	.lea_code <- .tmp$lea_code[1]
	.lea_name <- .tmp$lea_name[1]
	.school_name <- .tmp$school_name[1]
	
	newfile <- file("overview.json", encoding="UTF-8")

	if(!is.na(.tmp$profile_name[1])){
		.prof_name <- .tmp$profile_name[1]
	} else{
		.prof_name <- .tmp$school_name[1]
	}
	
	sink(file=newfile)
	cat('{', fill=TRUE)
	
	level <- 1
	cat(indent(level),'"timestamp": "',date(),'",', sep="", fill=TRUE)
	cat(indent(level),'"org_type": "school",', sep="", fill=TRUE)
	cat(indent(level),'"org_name": "',.school_name,'",', sep="", fill=TRUE)
	cat(indent(level),'"org_code": "',.org_code,'",', sep="", fill=TRUE)
	cat(indent(level),'"closed": ',JSONTrueFalse(.tmp$closing[1]), ',', sep="", fill=TRUE)
	cstat <- JSONTrueFalse(.tmp$charter_status[1])
	cat(sprintf('%s"charter": %s,',indent(level),cstat), fill=TRUE)
	cat(sprintf('%s"school_type": [',indent(level)), fill=TRUE)
	level <- level + 1

	## FILL SCHOOL_TYPE VAR ##
	cat(indent(level), '"',.tmp$school_type[1],'"', sep="", fill=TRUE)
	level <- level - 1
	cat(indent(level), '],', fill=TRUE)
	
	# if(.tmp$lea_code[1] !='0001'){
	# 	#PCSB PMF
	# 	cat(indent(level),'"pmf": {', sep="", fill=TRUE)
	# 	up(level)
	# 	cat(indent(level), '"id": "pcsb_pmf",', sep="", fill=TRUE)
	# 	cat(indent(level), '"data": [', sep="", fill=TRUE)
	# 	cat(ExPMF(.org_code, level+1), fill=TRUE)		
	# 	cat(indent(level), ']', sep="", fill=TRUE)
	# 	down(level)
	# 	cat(indent(level),'},', sep="", fill=TRUE)
	# }
	
	{
		#Great Schools PMF
		cat(indent(level),'"great_schools": {', sep="", fill=TRUE)
		up(level)
		cat(indent(level), '"gs_url": ', checkna_str(GSUrl(.org_code)), sep="", fill=TRUE)
		down(level)
		cat(indent(level),'},', sep="", fill=TRUE)
	}
	
	cat(indent(level),sprintf('"ward": %s,',checkna_str(substr(.tmp$ward[1], 6,6))), sep="", fill=TRUE) 	
	cat(indent(level),sprintf('"grades_serviced": [%s],', GetGrades(.org_code)), sep="", fill=TRUE) 
	cat(indent(level),sprintf('"grades_accepted": [%s],', GradesAccepted(.org_code)), sep="", fill=TRUE) 
	cat(indent(level) %+% '"description": "' %+% .tmp$description[1] %+% '",', fill=TRUE)
	
	cat(indent(level), '"address": {', fill=TRUE)
	level <- level + 1
	
	##checkna(school_dir$address_1[i])
	cat(indent(level),'"line_1": ',checkna_str(.tmp$address_1[1]),',', sep="", fill=TRUE)  
	cat(indent(level),'"line_2": ',checkna_str(.tmp$address_2[1]),',', sep="", fill=TRUE) 
	cat(indent(level),'"city": "Washington",', sep="", fill=TRUE) 
	cat(indent(level),'"state": "DC",', sep="", fill=TRUE) 
	cat(indent(level),'"zip": ',checkna_str(.tmp$zipcode[1]),',', sep="", fill=TRUE)  
	cat(indent(level),'"lat": ',checkna(.tmp$latitude[1]),',', sep="", fill=TRUE) 
	cat(indent(level),'"long": ',checkna(.tmp$longitude[1]), sep="", fill=TRUE)  
	
	level <- level - 1
	cat(indent(level), '},', fill=TRUE)

	cat(indent(level) %+% '"transit": "' %+% .tmp$routes[1] %+% '",', fill=TRUE)
	cat(indent(level) %+% '"website": ' %+% checkna_str(.tmp$website[1]) %+% ',', fill=TRUE) 
	cat(indent(level),'"facebook": ',checkna_str(.tmp$facebook[1]),',', sep="", fill=TRUE)
	cat(indent(level),'"twitter": ',checkna_str(.tmp$twitter[1]),',', sep="", fill=TRUE)  
	cat(indent(level),'"external_report_card": ',checkna_str(WriteProfile(.org_code)),',', sep="", fill=TRUE)  
	cat(indent(level),'"equity_report_url": \n',checkna_str(EquityUrl(.org_code)),',\n', sep="", fill=FALSE) 

	cat(indent(level), '"contact": [', sep="", fill=TRUE)
	cat(InsertPeople(.org_code, level+1))
	level <- level + 1
	cat('\n', indent(level), ']', sep='', fill=TRUE)
	
	level <- level - 1
	cat(indent(level), '}', sep='', fill=TRUE)
	sink()
	close(newfile)
}