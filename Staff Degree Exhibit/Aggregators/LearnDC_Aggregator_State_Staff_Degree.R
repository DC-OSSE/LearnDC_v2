setwd("U:/LearnDC ETL V2/Staff Degree Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(dplyr)

school_degree <- sqlFetch(dbrepcard_prod,"dbo.staff_degree_school_exhibit_bk")

state_degree <- data.frame()
	for(a in unique(school_degree$teacher_category)){
		degree_state <- subset(school_degree,teacher_category==a)
		teacher_category <- a

		for(b in unique(degree_state$year)){
	 		tmp <- subset(degree_state,year==b)
			year <- b
		
			total <- sum(tmp$num_total)
			associates <- sum(tmp$num_associates)
			bachelors <- sum(tmp$num_bachelors)
			masters <- sum(tmp$num_masters)
			phd <- sum(tmp$num_phd)
			none <- sum(tmp$num_none)

new_row <- c(year,teacher_category,associates,bachelors,masters,phd,none,total)
state_degree <- rbind(state_degree,new_row)
		}
	}
colnames(state_degree) <- c("year","teacher_category","num_associates","num_bachelors","num_masters","num_phd","num_none","num_total")

sqlSave(dbrepcard_prod, state_degree, tablename = "staff_degree_state_exhibit_bk", append = FALSE, rownames=FALSE)