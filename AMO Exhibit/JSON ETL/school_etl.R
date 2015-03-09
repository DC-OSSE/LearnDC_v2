setwd("U:/LearnDC ETL V2/AMO Exhibit/JSON ETL")
source("U:/R/tomkit.R")
library(dplyr)
library(jsonlite)

school_amo <- sqlQuery(dbrepcard,"SELECT * FROM [dbo].[amo_status_current_school]")

school_amo$enrollment_status <- "full_year"
school_amo$grade <- "all"
school_amo$subgroup[which(school_amo$subgroup=='African American')] <- 'BL7'
school_amo$subgroup[which(school_amo$subgroup=='Asian')] <- 'AS7'
school_amo$subgroup[which(school_amo$subgroup=='Economically Disadvantaged')] <- 'ECONOMY'
school_amo$subgroup[which(school_amo$subgroup=='English Learner')] <- 'LEP'
school_amo$subgroup[which(school_amo$subgroup=='Special Education')] <- 'SPED'
school_amo$subgroup[which(school_amo$subgroup=='Hispanic')] <- 'HI7'
school_amo$subgroup[which(school_amo$subgroup=='White')] <- 'WH7'
school_amo$subgroup[which(school_amo$subgroup=='Multiracial')] <- 'MU7'

school_amo_m <- select(school_amo, year, current_entity_code, current_entity_name,subgroup, grade, enrollment_status, math_tested, math_baseline, math_target)
school_amo_r <- select(school_amo, year, current_entity_code, current_entity_name,subgroup, grade, enrollment_status, read_tested, read_baseline, read_target)

school_amo_m$subject <- "Math"
school_amo_r$subject <- "Reading"
colnames(school_amo_m) <- c("year","school_code","school_name","subgroup","grade","enrollment_status","n_test_takers","baseline","target","subject")
colnames(school_amo_r) <- c("year","school_code","school_name","subgroup","grade","enrollment_status","n_test_takers","baseline","target","subject")

amo_school <- rbind(school_amo_m, school_amo_r)
amo_school <- select(amo_school, year, school_code, school_name, subgroup, subject, grade, enrollment_status, n_test_takers, baseline, target)

# setwd('U:/LearnDC ETL V2/Export/CSV/school')
# write.csv(amo_school, "amo_school.csv", row.names=FALSE)

# school_mapping <- sqlQuery(dbrepcard,"select * from dbo.school_mapping_sy1314")
# school_mapping$year <- school_mapping$ea_year + 1
# school_mapping <- select(school_mapping,year,school_code,sy1314_school_code,sy1314_school_name)

# amo_school <- merge(amo_school,school_mapping,by=c('year','school_code'),all.x=TRUE)

amo_school$school_code <- sapply(amo_school$school_code, leadgr, 4)

amo_school <- subset(amo_school,n_test_takers>=25)

key_index <- c(5:7,4,1)
value_index <- 9:10
num_orphans <- 0

for(i in unique(amo_school$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}

.tmp <- subset(amo_school, school_code == i)
.school_name <- .tmp$school_name[1]

.nested_list <- lapply(1:nrow(.tmp), FUN = function(i){ 
                             list(key = list(.tmp[i,key_index]), 
                             	val = list(.tmp[i,value_index]))
                           })


.json <- toJSON(.nested_list, na="null")
.json <- gsub("[[","",.json, fixed=TRUE)
.json <- gsub("]]","",.json, fixed=TRUE)
.json <- prettify(.json)

## write to file
newfile <- file("amo_targets.json", encoding="UTF-8")

sink(newfile)
cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "school",', sep="", fill=TRUE)
cat('"org_name": "',gsub("\n", "",.school_name),'",', sep="", fill=TRUE)
cat('"org_code": "',i,'",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "amo_targets",', fill=TRUE)
cat('\t"data": ',.json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)



sink()
close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))