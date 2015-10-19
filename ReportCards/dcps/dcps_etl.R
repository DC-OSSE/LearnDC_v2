## DCPS ETL
active <- shell('echo %HOMEPATH%', intern=TRUE)
active <- gsub('\\\\', '/', active)
setwd(paste0('C:', active, '/Documents/Github/ReportCards'))

source("./imports/tomkit.R")
source("http://www.straydots.com/code/ODBC.R")

library(xlsx)


school_dir <- sqlFetch(dbrepcard, 'schooldir_linked')

dcps_newdat <- read.xlsx("./dcps/ScoreCard_Metric_Values.xlsx", 1, encoding='UTF-8')
dcps_newdat <- subset(dcps_newdat, Dir_School_Code > 99)
dcps_newdat$Dir_School_Code <- sapply(dcps_newdat$Dir_School_Code, leadgr, 4)


## Update Profile Names
prof_names <- sqlFetch(dbrepcard, "profile_name_sy1314")
prof_names$school_code <- sapply(prof_names$school_code, leadgr, 4)

## add schools...
## No Schools Needed to be added.  

## Update Profile Names/Types

dcps_chunk <- subset(dcps_newdat, Dir_School_Code %in% prof_names$school_code, c("Dir_School_Code", "Dir_Name_Display", "Highlight"))

names(dcps_chunk) <- names(prof_names)

prof_keep <- subset(prof_names, school_code %notin% dcps_chunk$school_code)
prof_update <- rbind(prof_keep, dcps_chunk)

sqlUpdate(dbrepcard, prof_update, "profile_name_sy1314", index="school_code")


## Update People.
peeps <- sqlFetch(dbrepcard, "people_sy1314")

peeps$school_code <- sapply(peeps$school_code, leadgr, 4)


dcps_chunk <- subset(dcps_newdat, Dir_School_Code %in% peeps$school_code, c("Dir_School_Code", "Dir_Name_Display", "Principal_Name", "Principal_Name", "Dir_Phone", "Principal_Email"))
dcps_chunk$default <- 0


names(dcps_chunk) <- names(peeps)
dcps_chunk$role <- "Principal"

dcps_chunk$contact_name <- sapply(dcps_chunk$contact_name, trimall)
dcps_chunk$contact_email <- sapply(dcps_chunk$contact_email, trimall)

dcps_chunk$contact_name <- gsub("\n", "", dcps_chunk$contact_name)
dcps_chunk$contact_email <- gsub("\n", "", dcps_chunk$contact_email)

peeps_keep <- subset(peeps, school_code %notin% dcps_chunk$school_code)

peeps_update <- rbind(peeps_keep, dcps_chunk)

sqlUpdate(dbrepcard, peeps_update, "people_sy1314", index=c("school_code", "role"))

## Directory_Add ## 

school_dir <- sqlFetch(dbrepcard, "schooldir_sy1314")
school_dir$lea_code <- sapply(school_dir$lea_code, leadgr, 4)
school_dir$school_code <- sapply(school_dir$school_code, leadgr, 4)

dir_add <- dcps_newdat[,c("Dir_School_Code", "Dir_Website", "Dir_Street", "Dir_Facebook")]

school_dir_order <- names(school_dir)

school_dir2 <- merge(school_dir, dir_add, by.x="school_code", by.y="Dir_School_Code", all.x=TRUE)

school_dir2$Dir_Website <- gsub("\r", "", sapply(school_dir2$Dir_Website, trimall))

school_dir2$website[!is.na(school_dir2$Dir_Website)] <- school_dir2$Dir_Website[!is.na(school_dir2$Dir_Website)]

school_dir2$facebook[!is.na(school_dir2$Dir_Facebook)] <- school_dir2$Dir_Facebook[!is.na(school_dir2$Dir_Facebook)]


school_dir2$address_1[!is.na(school_dir2$Dir_Street)] <- school_dir2$Dir_Street[!is.na(school_dir2$Dir_Street)]

school_dir2 <- school_dir2[,school_dir_order]

sqlUpdate(dbrepcard, school_dir2, "schooldir_sy1314", index="school_code")

## Description Add ##
##mdata <- melt(mydata, id=c("id","time"))

school_progs <- sqlFetch(dbrepcard, "school_programs")
school_progs$school_code <- sapply(school_progs$school_code, leadgr, 4)

school_progs <- subset(school_progs, school_code %notin% dcps_newdat$Dir_School_code)

prog_load <- c("Dir_School_Code","Aca_Enr_1","Aca_Enr_2","Aca_Enr_3","Aca_Enr_4","Aca_Enr_5","Well_Fit_1","Well_Fit_2","Well_Fit_3","Well_Fit_4","Well_Fit_5","Art_Cult_1","Art_Cult_2","Art_Cult_3","Art_Cult_4","Art_Cult_5")


dcps_prog_chunk <- melt(dcps_newdat[,c(prog_load)], id="Dir_School_Code")

dcps_add <- dcps_prog_chunk[,c("Dir_School_Code", "value")]
names(dcps_add) <- names(school_progs)

school_progs <- rbind(school_progs, dcps_add)
school_progs <- subset(school_progs, !is.na(program_string))
sqlSave(dbrepcard, school_progs, tablename="school_programs", safer=FALSE, rownames=FALSE)


