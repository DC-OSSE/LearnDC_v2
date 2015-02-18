source("U:/R/tomkit.R")
library(dplyr)
library(jsonlite)

acc_1314 <- sqlQuery(dbrepcard,"select * from dbo.accountability_sy1314")
acc_1213 <- sqlQuery(dbrepcard,"select * from dbo.accountability_sy1213")
acc_a <- sqlQuery(dbrepcard,"select * from dbo.accountability_sy1314_sg")
acc_b <- sqlQuery(dbrepcard,"select * from dbo.accountability_sy1213_sg")
acc_a$year <- 2014
acc_b$year <- 2013

acc_1 <- rbind(acc_1213,acc_1314)
acc_2 <- rbind(acc_a,acc_b)

acc_all <- merge(acc_2,acc_1,by=c("school_code","year","lea_code","lea_name"),all.x=TRUE)
names(acc_all)[13] <- "school_name"
acc_all$subject <- NA
acc_read <- select(acc_all,year,lea_code,lea_name,school_code,school_name,subject,subgroup,read_score,read_size,classification,acct_score,growth)
acc_read$subject <- "Reading"
acc_math <- select(acc_all,year,lea_code,lea_name,school_code,school_name,subject,subgroup,math_score,math_size,classification,acct_score,growth)
acc_math$subject <- "Math"
acc_comp <- select(acc_all,year,lea_code,lea_name,school_code,school_name,subject,subgroup,comp_score,comp_size,classification,acct_score,growth)
acc_comp$subject <- "Composition"


colnames(acc_comp) <- c("year","lea_code","lea_name","school_code","school_name","subject","subgroup","subject_score","subject_size","classification","acct_score","growth")
colnames(acc_read) <- c("year","lea_code","lea_name","school_code","school_name","subject","subgroup","subject_score","subject_size","classification","acct_score","growth")
colnames(acc_math) <- c("year","lea_code","lea_name","school_code","school_name","subject","subgroup","subject_score","subject_size","classification","acct_score","growth")

acc_school <- rbind(acc_read,acc_math,acc_comp)
acc_school$school_code <- sapply(acc_school$school_code, leadgr, 4)
acc_school$lea_code <- sapply(acc_school$lea_code, leadgr, 4)

acc_school <- subset(acc_school,subject_size>=25)

acc_school$subgroup[which(acc_school$subgroup=='African American')] <- 'BL7'
acc_school$subgroup[which(acc_school$subgroup=='Asian')] <- 'AS7'
acc_school$subgroup[which(acc_school$subgroup=='Economically Disadvantaged')] <- 'ECONOMY'
acc_school$subgroup[which(acc_school$subgroup=='English Learner')] <- 'LEP'
acc_school$subgroup[which(acc_school$subgroup=='Special Education')] <- 'SPED'
acc_school$subgroup[which(acc_school$subgroup=='Hispanic')] <- 'HI7'
acc_school$subgroup[which(acc_school$subgroup=='White')] <- 'WH7'
acc_school$subgroup[which(acc_school$subgroup=='Multiracial')] <- 'MU7'
acc_school$subgroup[which(acc_school$subgroup=='Multi-Ethnic')] <- 'MU7'
acc_school$subgroup[which(acc_school$subgroup=='Male')] <- 'MALE'
acc_school$subgroup[which(acc_school$subgroup=='Female')] <- 'FEMALE'


key_index <- c(1,6,7)
value_index <- c(8,9,11,12,10)
num_orphans <- 0

for(i in unique(acc_school$school_code)){
	setwd("U:/LearnDC ETL V2/Export/JSON/school")

	if(file.exists(i)){
	    setwd(file.path(i))
	} else {
		num_orphans <- num_orphans + 1
	}

.tmp <- subset(acc_school, school_code == i)
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
newfile <- file("accountability.json", encoding="UTF-8")

sink(newfile)
cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "school",', sep="", fill=TRUE)
cat('"org_name": "',gsub("\n", "",.school_name),'",', sep="", fill=TRUE)
cat('"org_code": "',i,'",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "accountability",', fill=TRUE)
cat('\t"data": ',.json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)



sink()
close(newfile)
}

print(paste0("There are ",num_orphans," orphaned files."))