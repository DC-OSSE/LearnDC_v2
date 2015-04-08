setwd("U:/LearnDC ETL V2/Staff Degree Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(dplyr)

staff_degree <- sqlQuery(dbstaff,"select school_year,unique_id,elem_sec_teacher,lea_code,lea_name,school_code,school_name,phd_ind,masters_ind,bachelors_ind,aa_ind from dbo.education_credentials where elem_sec_teacher in ('elem','sec')")

staff_degree$year[which(staff_degree$school_year=='2013-2014')] <- 2014
staff_degree$school_code <- sapply(staff_degree$school_code, leadgr, 3)
staff_degree$lea_code <- sapply(staff_degree$lea_code, leadgr, 3)
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

staff_degree <- select(staff_degree,year,school_category=elem_sec_teacher,lea_code,lea_name,school_code,school_name,is_none,is_aa,is_bachelors,is_masters,is_phd)

state_degree <- data.frame()
	for(a in unique(staff_degree$teacher_category)){
		degree_state <- subset(staff_degree,teacher_category==a)
		teacher_category <- a

		for(b in unique(degree_state$year)){
	 		tmp <- subset(degree_state,year==b)
			year <- b
		
			total <- nrow(tmp)
			associates <- sum(tmp$is_aa)
			bachelors <- sum(tmp$is_bachelors)
			masters <- sum(tmp$is_masters)
			phd <- sum(tmp$is_phd)
			none <- sum(tmp$is_none)

new_row <- c(year,teacher_category,associates,bachelors,masters,phd,none,total)
state_degree <- rbind(state_degree,new_row)
		}
	}
colnames(state_degree) <- c("year","teacher_category","num_associates","num_bachelors","num_masters","num_phd","num_none","num_total")

sqlSave(dbrepcard_prod, state_degree, tablename = "staff_degree_state_exhibit", append = FALSE, rownames=FALSE)