setwd("U:/LearnDC ETL V2/AMO Exhibit/JSON ETL")
source("U:/R/tomkit.R")
library(dplyr)
library(jsonlite)

lea_amo <- sqlQuery(dbrepcard,"SELECT * FROM [dbo].[amo_status_current_lea]")

lea_amo$enrollment_status <- "full_year"
lea_amo$grade <- "all"
lea_amo$subgroup[which(lea_amo$subgroup=='African American')] <- 'BL7'
lea_amo$subgroup[which(lea_amo$subgroup=='Asian')] <- 'AS7'
lea_amo$subgroup[which(lea_amo$subgroup=='Economically Disadvantaged')] <- 'ECONOMY'
lea_amo$subgroup[which(lea_amo$subgroup=='English Learner')] <- 'LEP'
lea_amo$subgroup[which(lea_amo$subgroup=='Special Education')] <- 'SPED'
lea_amo$subgroup[which(lea_amo$subgroup=='Hispanic')] <- 'HI7'
lea_amo$subgroup[which(lea_amo$subgroup=='White')] <- 'WH7'
lea_amo$subgroup[which(lea_amo$subgroup=='Multiracial')] <- 'MU7'

lea_amo_m <- select(lea_amo, year, lea_code, lea_name,subgroup, grade, enrollment_status, math_tested, math_baseline, math_target)
lea_amo_r <- select(lea_amo, year, lea_code, lea_name,subgroup, grade, enrollment_status, read_tested, read_baseline, read_target)

lea_amo_m$subject <- "Math"
lea_amo_r$subject <- "Reading"
colnames(lea_amo_m) <- c("year","lea_code","lea_name","subgroup","grade","enrollment_status","n_test_takers","baseline","target","subject")
colnames(lea_amo_r) <- c("year","lea_code","lea_name","subgroup","grade","enrollment_status","n_test_takers","baseline","target","subject")

amo_lea <- rbind(lea_amo_m, lea_amo_r)
amo_lea <- select(amo_lea, year, lea_code, lea_name, subgroup, subject, grade, enrollment_status, n_test_takers, baseline, target)

setwd('U:/LearnDC ETL V2/Export/CSV/lea')
write.csv(amo_lea, "amo_lea.csv", row.names=FALSE)

amo_lea$lea_code <- sapply(amo_lea$lea_code, leadgr, 4)

amo_lea <- subset(amo_lea,n_test_takers>=25)

key_index <- c(5:7,4,1)
value_index <- 9:10
num_orphans <- 0

for(i in unique(amo_lea$lea_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/lea")

	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}

.tmp <- subset(amo_lea, lea_code == i)
.lea_name <- .tmp$lea_name[1]

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
cat('"org_type": "lea",', sep="", fill=TRUE)
cat('"org_name": "',gsub("\n", "",.lea_name),'",', sep="", fill=TRUE)
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