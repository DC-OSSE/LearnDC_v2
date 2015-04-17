setwd("U:/LearnDC ETL V2/Staff Degree Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(dplyr)

staff_degree <- sqlFetch(dbrepcard_prod,"dbo.staff_degree_school_exhibit")
staff_degree$year[which(staff_degree$school_year=='2013-2014')] <- 2014
staff_degree$lea_code <- sapply(staff_degree$lea_code, leadgr, 3)
staff_degree$lea_name <- toupper(staff_degree$lea_name)
staff_degree$lea_name[which(staff_degree$lea_code!='001')] <- "PUBLIC CHARTER SCHOOLS"
staff_degree$lea_code[which(staff_degree$lea_code!='001')] <- "000"

sector_degree <- data.frame()
for(a in unique(staff_degree$lea_code)){
	degree_sector <- subset(staff_degree,lea_code==a)
	lea_code <- a
	lea_name <- degree_sector$lea_name[1]

	for(b in unique(degree_sector$teacher_category)){
		degree_sector_type <- subset(degree_sector,teacher_category==b)
		teacher_category <- b

		for(c in unique(degree_sector_type$year)){
	 		tmp <- subset(degree_sector_type,year==c)
			year <- c
		
			total <- sum(tmp$num_total)
			associates <- sum(tmp$num_associates)
			bachelors <- sum(tmp$num_bachelors)
			masters <- sum(tmp$num_masters)
			phd <- sum(tmp$num_phd)
			none <- sum(tmp$num_none)

new_row <- c(year,lea_code,lea_name,teacher_category,associates,bachelors,masters,phd,none,total)
sector_degree <- rbind(sector_degree,new_row)
		}
	}
}
colnames(sector_degree) <- c("year","lea_code","lea_name","teacher_category","num_associates","num_bachelors","num_masters","num_phd","num_none","num_total")

sqlSave(dbrepcard_prod, sector_degree, tablename = "staff_degree_sector_exhibit", append = FALSE, rownames=FALSE)