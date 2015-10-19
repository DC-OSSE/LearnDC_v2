## Batchable Export of JSON Data
## Generate School File ## 

## package loading
library(xlsx)

curr_dir <- getwd()

## active directory
active <- shell('echo %HOMEPATH%', intern=TRUE)
active <- gsub('\\\\', '/', active)
setwd(paste0('C:', active, '/Documents/Github/ReportCards'))

## imports
source("./imports/tomkit.R")
source("http://www.straydots.com/code/ODBC.R")
source("./school_functions.R")
##source("./state_functions.R")

setwd(curr_dir)

SchoolFlatFile <- function(org_code){
	school_dir <- sqlQuery(dbrepcard, sprintf("SELECT * 
		FROM [dbo].[schooldir_linked]
		WHERE [school_code] = '%s'", leadgr(org_code,4)))

	if(nrow(school_dir)<1){
		cat("No School Found", fill=TRUE)	
	} else if(nrow(school_dir)>1){
		print(school_dir)
		cat("Multiple Schools Found", fill=TRUE)
	} else{	
		school_dir$school_code <- sapply(school_dir$school_code, leadgr, 4)
		school_dir$lea_code <- sapply(school_dir$lea_code, leadgr, 4)
			
		filename <- paste0(school_dir$school_code[1], school_dir$school_name[1], "_overview.xlsx")

		cat("Generating File...", fill=TRUE)
		outfile <- StartFlatFile(school_dir)
		
		cat("Exporting File...", fill=TRUE)
		
		saveWorkbook(outfile, filename)
	}
}

StartFlatFile <- function(dirline){	
	org_code <- dirline$school_code[1]	
	flatbase <- createWorkbook()
	flatbase <- BuildOverview(flatbase, dirline)
	
	header_style <- CellStyle(flatbase) + Font(flatbase, isBold=TRUE) + Border()
	## Enrollment 
	{
		enrollment_sheet <- createSheet(flatbase, sheetName="Enrollment")
		enroll_tab <- GetEnrollmentDF(org_code)
		
		addDataFrame(enroll_tab, enrollment_sheet, row.names=FALSE, startRow=1, startColumn=1, colnamesStyle=header_style)

		autoSizeColumn(enrollment_sheet, 1:4)
	}
	
	## Assessment 
	{
		assessment_sheet <- createSheet(flatbase, sheetName="Assessment")
		assess_tab <- GetAssessmentDF(org_code)
		
		addDataFrame(assess_tab, assessment_sheet, row.names=FALSE, startRow=1, startColumn=1, colnamesStyle=header_style)

		autoSizeColumn(assessment_sheet, 1:12)
	}
	
	return(flatbase)
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

BuildOverview <- function(wb, dirline){

	org_code <- dirline$school_code[1]	
	overview_sheet <- createSheet(wb, sheetName="Overview")

	rows <- createRow(overview_sheet, 1:20)
	cells <- createCell(rows, colIndex=1:4)
	
	.stitle <- dirline$school_name[1]
	title_style <- CellStyle(wb) + Font(wb, isBold=TRUE, heightInPoints=16, color="#2222A1") + Border()
	
	setCellValue(cells[[1,1]], .stitle)
	addMergedRegion(overview_sheet, 1, 1, 1, 2)
	setCellStyle(cells[[1,1]], title_style)
	setCellStyle(cells[[1,2]], title_style)
		
	header_style2 <- CellStyle(wb) + Font(wb, isBold=TRUE) 
	
	setCellValue(cells[[3,1]], "org_type:")
	setCellValue(cells[[3,2]], "school")
	
	setCellValue(cells[[4,1]], "charter:")
	setCellValue(cells[[4,2]], checkna_str(dirline$charter_status[1]))
	
	setCellValue(cells[[5,1]], "great_schools:")
	setCellValue(cells[[5,2]], checkna_str(GSUrl(org_code)))
	
	setCellValue(cells[[7,1]], "address_1")
	setCellValue(cells[[7,2]], checkna_str(dirline$address_1[1]))
	setCellValue(cells[[8,1]], "address_2")
	setCellValue(cells[[8,2]], checkna_str(dirline$address_2[1]))
	setCellValue(cells[[9,1]], "city:")
	setCellValue(cells[[9,2]], "Washington")
	setCellValue(cells[[10,1]], "state:")
	setCellValue(cells[[10,2]], "DC")
	setCellValue(cells[[11,1]], "zip:")
	setCellValue(cells[[11,2]], checkna_str(dirline$zipcode[1]))
	setCellValue(cells[[12,1]], "latitude:")
	setCellValue(cells[[12,2]], checkna_str(dirline$latitude[1]))
	setCellValue(cells[[13,1]], "longitude:")
	setCellValue(cells[[13,2]], checkna_str(dirline$longitude[1]))
	
	for(i in 3:13){
		setCellStyle(cells[[i,1]], header_style2)
	}
	
	autoSizeColumn(overview_sheet, 1:2)
	
	return(wb)
}

## GET FLAT CAS DATA FOR SCHOOL SELECTED
GetAssessmentDF <- function(scode){
	## MATH/READING
	.proc <- list()
	
	.tblist <- c("assessment_sy0910", "assessment_sy1011", "assessment_sy1112", "assessment_sy1213")
	
	for(i in .tblist){
		.qry <- sprintf("SELECT * FROM [dbo].[%s]
		WHERE [fy13_entity_code] = '%s';", i, leadgr(scode,4))
		.proc[[i]] <- sqlQuery(dbrepcard, .qry)	
	}
	for(i in .proc){
		if(exists('.retdf')){
			.retdf <- rbind(.retdf, BuildCASDF(i))
		} else{
			.retdf <- BuildCASDF(i)
		}
	}
	return(.retdf)
}

BuildCASDF <- function(.casdat_mr){
	.cas_row <- data.frame(matrix(nrow=1, ncol=11))
	names(.cas_row) <- c("year", "enrollment_status", "grade", "subgroup", "subject","n_eligible", "n_test_takers", "below_basic", "basic", "proficient", "advanced")
	
	if(is.null(.casdat_mr) | is.logical(.casdat_mr)){
		return(.cas_row[NULL,]) 
	}
	
	.casdat_mr$math_level[.casdat_mr$exclude=='I'] <- NA
	.casdat_mr$read_level[.casdat_mr$exclude=='I'] <- NA

	if(nrow(.casdat_mr) <10) {
		##print("NSIZE")
		return(.cas_row[NULL,]) 
	}
	
	.catch <- .cas_row[NULL,]
	
	.subjects <- c("Math", "Reading")
	.fay <- c("all", "full_year")
	
	.plevels <- c("Below Basic", "Basic", "Proficient", "Advanced")
	.glevels <- sort(unique(.casdat_mr$tested_grade))
	
	## A = Subject, 1 for Math, 2 for Reading
	for(a in 1:2){
		## b = full year or not
		for(b in 1:2){
			## d = each grade 
			for(g in 0:length(.glevels)){
				goutput <- ''
				.tmp <- .casdat_mr
				
				if(g == 0){
					goutput <- 'all'
				} else{
					goutput <- paste('grade', .glevels[g], sep=" ")
					.tmp <- subset(.tmp, tested_grade==.glevels[g])
				}
				.flevels <- c("N", "S", "D", "C")
				
				if(b==2){
					## Detection of State/District/School Level
					
					if(length(unique(.tmp$lea_code))>1){
						.flevels <- c("S", "C", "D")
					} else if(length(unique(.tmp$fy13_entity_code))>1){
						.flevels <- c("S", "C")
					} else{
						.flevels <- c("S")
					}				
					
					.tmp <- subset(.tmp, new_to_us =='NO')
					.tmp <- subset(.tmp, school_grade==tested_grade | alt_tested=="YES")
				}

				.subgroups <- c("African American","White","Hispanic","Asian", "Pacific Islander", "Multiracial","Special Education","English Learner","Economically Disadvantaged","Male", "Female")
				
				for(h in 0:9){
					.tmps <- SubProc(.tmp, h, b)
					
					if(h == 0){
						soutput <- 'All'
					} else{
						soutput <- .subgroups[h]
					}
					
					if((nrow(.tmps)>=10 & b==1) | (nrow(.tmps)>=25 & b==2)){
						.add <- .cas_row
						
						if(a ==1){
							.profs <- .tmps$math_level[.tmps$full_academic_year %in% .flevels]
						} else if(a == 2){
							.profs <- .tmps$read_level[.tmps$full_academic_year %in% .flevels]
						}

						.add$year <- .casdat_mr$year[1]
						.add$subject <- .subjects[a]
						.add$grade <- goutput
						.add$subgroup <- soutput
						.add$enrollment_status <- .fay[b]
						
						.add$n_eligible <- length(.profs)
						.add$n_test_takers <- length(.profs[.profs %in% .plevels])
						
						.add$below_basic <- length(.profs[.profs %in% "Below Basic"])
						.add$basic <- length(.profs[.profs %in% "Basic"])
						.add$proficient <- length(.profs[.profs %in% "Proficient"])
						.add$advanced <- length(.profs[.profs %in% "Advanced"])
						
						##print(.add)
						.catch <- rbind(.catch, .add)
					}
				}
			}
		}
	}
	return(.catch)
}

## POPULATE FLAT FILE OF ENROLLMENT DATA FOR SCHOOL SELECTED
GetEnrollmentDF <- function(scode){
	## MATH/READING
	.proc <- list()
	
	.tblist <- c("enrollment_sy0607", "enrollment_sy0708", "enrollment_sy0809", "enrollment_sy0910", "enrollment_sy1011", "enrollment_sy1112", "enrollment_sy1213")
	
	for(i in .tblist){
		.qry <- sprintf("SELECT * FROM [dbo].[%s]
		WHERE [fy13_entity_code] = '%s';", i, leadgr(scode,4))
		.proc[[i]] <- sqlQuery(dbrepcard, .qry)	
	}
	
	for(i in .proc){
		if(exists('.retdf')){
			.retdf <- rbind(.retdf, BuildEnrDF(i))
		} else{
			.retdf <- BuildEnrDF(i)
		}
	}	
	return(.retdf)
}


BuildEnrDF <- function(.enr_dat){
	.enr_row <- data.frame(matrix(nrow=1, ncol=4))
	names(.enr_row) <- c("year", "grade", "subgroup", "students")
	
	if(is.null(.enr_dat) | is.logical(.enr_dat)){
		return(.enr_row[NULL,]) 
	}
	
	year <- .enr_dat$ea_year[1]	
	.encatch <- .enr_row[NULL,]
		
	.enr_dat$grade <- sapply(.enr_dat$grade, leadgr, 2)	
	.glist <- unique(.enr_dat$grade)
	
	.subgroups <- c("African American","White","Hispanic","Asian","American Indian", "Pacific Islander", "Multi Racial","Special Education","English Learner","Economically Disadvantaged","Male", "Female")
	
	for(g in 0:length(.glist)){
		.tmp <- .enr_dat
		goutput <- "All"
		if(g>0){
			.tmp <- subset(.tmp, grade==.glist[g])
			goutput <- .glist[g]
		}
		
		for(s in 0:length(.subgroups)){
			soutput <- "All"
			.tmps <- .tmp
			if(s > 0){
				soutput <- .subgroups[s]
				.tmps <- SubProcEnr(.tmp, s)
			}
			if(nrow(.tmps)>=10){
				.add <- .enr_row
				
				.add$year <- year
				.add$grade <- goutput
				.add$subgroup <- soutput
				.add$students <- nrow(.tmps)
				
				if(exists('.encatch')){
					.encatch <- rbind(.encatch, .add)
				} else{
					.encatch <- .add
				}
			}
		}
	}
	return(.encatch)
}


