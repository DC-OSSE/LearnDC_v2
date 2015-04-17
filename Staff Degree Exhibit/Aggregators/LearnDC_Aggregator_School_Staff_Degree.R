setwd("U:/LearnDC ETL V2/Staff Degree Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(dplyr)

school_dir <- sqlQuery(dbrepcard,"select * from dbo.schooldir_sy1314")
school_dir <- select(school_dir,year=school_year,lea_code,lea_name,school_code,school_name)
school_dir$year <- 2014
school_dir$school_code <- sapply(school_dir$school_code, leadgr, 3)
school_dir$lea_code <- sapply(school_dir$lea_code, leadgr, 3)

lookup_ent <- sqlQuery(dbedm,"select * from dbo.lookup_entities where school_year='2013-2014'")
lookup_ent <- select(lookup_ent,year=school_year,lea_code,school_code,elem_sec_teacher=elem_sec_classification)
lookup_ent$year <- 2014
lookup_ent$school_code <- sapply(lookup_ent$school_code, leadgr, 3)
lookup_ent$lea_code <- sapply(lookup_ent$lea_code, leadgr, 3)
school_dir <- merge(school_dir,lookup_ent,by=c('year','lea_code','school_code'),all.x=TRUE)
school_dir <- subset(school_dir,elem_sec_teacher %in% c('ELEM','SEC','SEC, ADULT'))
school_dir$elem_sec_teacher[which(school_dir$elem_sec_teacher %in% c('SEC, ADULT'))] <- 'SEC'

staff_degree <- sqlQuery(dbstaff,"select school_year,unique_id,elem_sec_teacher,lea_code,lea_name,school_code,school_name,phd_ind,masters_ind,bachelors_ind,aa_ind from dbo.education_credentials where elem_sec_teacher in ('elem','sec')")
staff_degree$year[which(staff_degree$school_year=='2013-2014')] <- 2014
staff_degree$school_code <- sapply(staff_degree$school_code, leadgr, 3)
staff_degree$lea_code <- sapply(staff_degree$lea_code, leadgr, 3)
staff_degree$school_name <- toupper(staff_degree$school_name)
staff_degree$lea_name <- toupper(staff_degree$lea_name)

school_dir$school_year <- '2013-2014'
school_dir$unique_id <- "N"
school_dir$phd_ind <- "N"
school_dir$masters_ind <- "N"
school_dir$bachelors_ind <- "N"
school_dir$aa_ind <- "N"
school_dir <- subset(school_dir,school_dir$school_code %notin% staff_degree$school_code)
##Schools included that may need to be subset out eventually:  walker jones, maya angelou, shaw ms, st coletta

staff_degree <- rbind(staff_degree,school_dir)

# staff_degree <- merge(school_dir,staff_degree,by=c("year","lea_code","school_code","elem_sec_teacher"),all.x=TRUE)
staff_degree <- select(staff_degree,year,unique_id,teacher_category=elem_sec_teacher,lea_code,lea_name,school_code,school_name,phd_ind,masters_ind,bachelors_ind,aa_ind)
staff_degree$school_name <- toupper(staff_degree$school_name)
staff_degree$lea_name <- toupper(staff_degree$lea_name)


staff_degree$is_phd <- 0
staff_degree$is_phd[which(staff_degree$phd_ind %in% c("Y","Y "))] <- 1
staff_degree$is_phd <- as.numeric(staff_degree$is_phd)

staff_degree$is_masters <- 0
staff_degree$is_masters[which(staff_degree$phd_ind=='N' & staff_degree$masters_ind %in% c("Y","Y "))] <- 1
staff_degree$is_masters <- as.numeric(staff_degree$is_masters)

staff_degree$is_bachelors <- 0
staff_degree$is_bachelors[which(staff_degree$phd_ind=='N' & staff_degree$masters_ind=='N' & staff_degree$bachelors_ind=='Y')] <- 1
staff_degree$is_bachelors <- as.numeric(staff_degree$is_bachelors)

staff_degree$is_aa <- 0
staff_degree$is_aa[which(staff_degree$phd_ind=='N' & staff_degree$masters_ind=='N' & staff_degree$bachelors_ind=='N' & staff_degree$aa_ind=='Y')] <- 1
staff_degree$is_aa <- as.numeric(staff_degree$is_aa)

staff_degree$is_none <- 0
staff_degree$is_none[which(staff_degree$phd_ind=='N' & staff_degree$masters_ind=='N' & staff_degree$bachelors_ind=='N' & staff_degree$aa_ind=='N')] <- 1
 staff_degree$is_none[which(staff_degree$is_phd==0 & staff_degree$is_masters==0 & staff_degree$is_bachelors==0 & staff_degree$is_aa==0 & staff_degree$is_none==0)] <- 1
 staff_degree$is_none <- as.numeric(staff_degree$is_none)

staff_degree <- select(staff_degree,year,unique_id,teacher_category,lea_code,lea_name,school_code,school_name,is_none,is_aa,is_bachelors,is_masters,is_phd)

school_degree <- data.frame()
for(a in unique(staff_degree$school_code)){
	degree_school <- subset(staff_degree,school_code==a)
	school_code <- a
	school_name <- degree_school$school_name[1]
	lea_code <- degree_school$lea_code[1]
	lea_name <- degree_school$lea_name[1]

	for(b in unique(degree_school$teacher_category)){
		degree_school_type <- subset(degree_school,teacher_category==b)
		teacher_category <- b

		for(c in unique(degree_school_type$year)){
	 		tmp <- subset(degree_school_type,year==c)
			year <- c
		
			total <- nrow(tmp)
			associates <- sum(tmp$is_aa)
			bachelors <- sum(tmp$is_bachelors)
			masters <- sum(tmp$is_masters)
			phd <- sum(tmp$is_phd)
			none <- sum(tmp$is_none)

new_row <- c(year,lea_code,lea_name,school_code,school_name,teacher_category,associates,bachelors,masters,phd,none,total)
school_degree <- rbind(school_degree,new_row)
		}
	}
}
colnames(school_degree) <- c("year","lea_code","lea_name","school_code","school_name","teacher_category","num_associates","num_bachelors","num_masters","num_phd","num_none","num_total")

sqlSave(dbrepcard_prod, school_degree, tablename = "staff_degree_school_exhibit", append = FALSE, rownames=FALSE)