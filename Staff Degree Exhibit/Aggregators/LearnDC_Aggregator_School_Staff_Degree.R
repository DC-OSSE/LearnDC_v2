setwd("U:/LearnDC ETL V2/Staff Degree Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(dplyr)

teacher_ed <- sqlFetch(dbrepcard,"dbo.education_credentials_wrevisions")
teacher_ed <- subset(teacher_ed,staff_category=='Teacher')
teacher_ed$highest_degree <- ifelse(teacher_ed$phd_ind %in% c('true','x','Y','Y ','Yes','YES'),'PhD',ifelse(teacher_ed$masters_ind %in% c('true','x','Y','Y ','yes','Yes','YES','M.Ed.','MA','MAT','MAT Art Education','MS'),'Masters',ifelse(teacher_ed$bachelors_ind %in% c('true','x','Y','Y ','yes','Yes','YES','y','BA',"Bachelor's",'BS'),'Bachelors',ifelse(teacher_ed$aa_ind %in% c('Y','yes','Yes'),'Associates','None'))))
teacher_ed <- teacher_ed[!duplicated(teacher_ed),]
teacher_ed$elem_sec_teacher <- ifelse(teacher_ed$staff_role=='Teacher, Elementary','ELEM',ifelse(teacher_ed$staff_role=='Teacher, Secondary','SEC',NA))
teacher_ed <- subset(teacher_ed,!is.na(elem_sec_teacher))
teacher_ed <- subset(teacher_ed,school_code %notin% c('none','343'))
teacher_ed$school_code <- sapply(teacher_ed$school_code,leadgr,3)
teacher_ed$lea_code <- sapply(teacher_ed$lea_code,leadgr,3)
teacher_ed$school_name <- toupper(teacher_ed$school_name)
teacher_ed$lea_name <- toupper(teacher_ed$lea_name)
teacher_ed$year <-ifelse(teacher_ed$school_year=='2013-2014',2014,NA)

school_degree <- data.frame()
for(a in unique(teacher_ed$school_code)){
    degree_school <- subset(teacher_ed,school_code==a)
    school_code <- a
    school_name <- degree_school$school_name[1]
    lea_code <- degree_school$lea_code[1]
    lea_name <- degree_school$lea_name[1]
    
    	for(b in unique(degree_school$elem_sec_teacher)){
    		teacher_degree <- subset(degree_school,elem_sec_teacher==b)
    		teacher_category <- b

    		for(c in unique(teacher_degree$year)){
    			tmp <- subset(teacher_degree,year==c)
    			year <- c

    			num_associates <- nrow(subset(tmp,highest_degree=='Associates'))
    			num_bachelors <- nrow(subset(tmp,highest_degree=='Bachelors'))
    			num_masters <- nrow(subset(tmp,highest_degree=='Masters'))
    			num_phd <- nrow(subset(tmp,highest_degree=='PhD'))
    			num_none <- nrow(subset(tmp,highest_degree=='None'))
    			num_total <- nrow(tmp)
    
    			new_row <- c(year,lea_code,lea_name,school_code,school_name,teacher_category,num_associates,num_bachelors,num_masters,num_phd,num_none,num_total)
    			school_degree <- rbind(school_degree,new_row)
		}
	}
}
colnames(school_degree) <- c("year","lea_code","lea_name","school_code","school_name","teacher_category","num_associates","num_bachelors","num_masters","num_phd","num_none","num_total")

school_dir <- sqlQuery(dbrepcard,"select * from dbo.schooldir_sy1314")
school_dir$year <- ifelse(school_dir$school_year=='2013-2014',2014,NA)
school_dir <- select(school_dir,year,lea_code,lea_name,school_code,school_name)
school_dir$school_code <- sapply(school_dir$school_code, leadgr, 3)
school_dir$lea_code <- sapply(school_dir$lea_code, leadgr, 3)

lookup_ent <- sqlQuery(dbedm,"select * from dbo.lookup_entities where school_year='2013-2014'")
lookup_ent <- select(lookup_ent,year=school_year,lea_code,school_code,teacher_category=elem_sec_classification)
lookup_ent$year <- 2014
lookup_ent$school_code <- sapply(lookup_ent$school_code, leadgr, 3)
lookup_ent$lea_code <- sapply(lookup_ent$lea_code, leadgr, 3)
school_dir <- merge(school_dir,lookup_ent,by=c('year','lea_code','school_code'),all.x=TRUE)

non_report <- subset(school_dir,school_dir$school_code %notin% school_degree$school_code)
non_report <- subset(non_report,lea_code %notin% c('103','162','119','176','143','145','131','133') & school_code %notin% c('958','6000','948','265','312','7000','472','465','209'))
non_report$school_name <- toupper(non_report$school_name)
non_report$lea_name <- toupper(non_report$lea_name)
non_report$num_associates <- 0
non_report$num_bachelors <- 0
non_report$num_masters <- 0
non_report$num_phd <- 0
non_report$num_none <- 1
non_report$num_total <- 1

school_degree <- rbind.data.frame(school_degree,non_report)

sqlSave(dbrepcard_prod,school_degree,tablename="staff_degree_school_exhibit",append = FALSE,rownames=FALSE)