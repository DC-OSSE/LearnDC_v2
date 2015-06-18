setwd("U:/LearnDC ETL V2/Staff Degree Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(dplyr)

school_degree <- sqlFetch(dbrepcard_prod,"dbo.staff_degree_school_exhibit")
school_degree$lea_code <- sapply(school_degree$lea_code, leadgr, 3)

lea_degree <- data.frame()
for(a in unique(school_degree$lea_code)){
	degree_lea <- subset(school_degree,lea_code==a)
	lea_code <- a
	lea_name <- degree_lea$lea_name[1]

	for(b in unique(degree_lea$teacher_category)){
		degree_lea_type <- subset(degree_lea,teacher_category==b)
		teacher_category <- b

		for(c in unique(degree_lea_type$year)){
	 		tmp <- subset(degree_lea_type,year==c)
			year <- c
		
			associates <- sum(tmp$num_associates,na.rm=TRUE)
			bachelors <- sum(tmp$num_bachelors,na.rm=TRUE)
			masters <- sum(tmp$num_masters,na.rm=TRUE)
			phd <- sum(tmp$num_phd,na.rm=TRUE)
			none <- sum(tmp$num_none,na.rm=TRUE)
			total <- sum(tmp$num_total,na.rm=TRUE)

new_row <- c(year,lea_code,lea_name,teacher_category,associates,bachelors,masters,phd,none,total)
lea_degree <- rbind(lea_degree,new_row)
		}
	}
}
colnames(lea_degree) <- c("year","lea_code","lea_name","teacher_category","num_associates","num_bachelors","num_masters","num_phd","num_none","num_total")

sqlSave(dbrepcard_prod,lea_degree,tablename="staff_degree_lea_exhibit",append=FALSE,rownames=FALSE)