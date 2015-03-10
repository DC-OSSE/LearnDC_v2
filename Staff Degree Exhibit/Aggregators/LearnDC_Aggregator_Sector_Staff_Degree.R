setwd("U:/LearnDC ETL V2/Staff Degree Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(dplyr)

staff_degree <- sqlFetch(dbworking,"staff_degree_sy1314_test")
staff_degree$year[which(staff_degree$school_year=='2013-2014')] <- 2014
staff_degree$is_aa[which(staff_degree$is_aa=='N')] <- 0
staff_degree$is_aa[which(staff_degree$is_aa=='Y')] <- 1
staff_degree$is_aa <- as.numeric(staff_degree$is_aa)
staff_degree$is_bachelors[which(staff_degree$is_bachelors=='N')] <- 0
staff_degree$is_bachelors[which(staff_degree$is_bachelors=='Y')] <- 1
staff_degree$is_bachelors <- as.numeric(staff_degree$is_bachelors)
staff_degree$is_masters[which(staff_degree$is_masters=='N')] <- 0
staff_degree$is_masters[which(staff_degree$is_masters=='Y')] <- 1
staff_degree$is_masters <- as.numeric(staff_degree$is_masters)
staff_degree$is_phd[which(staff_degree$is_phd=='N')] <- 0
staff_degree$is_phd[which(staff_degree$is_phd=='Y')] <- 1
staff_degree$is_phd <- as.numeric(staff_degree$is_phd)
staff_degree$is_none[which(staff_degree$is_none=='N')] <- 0
staff_degree$is_none[which(staff_degree$is_none=='Y')] <- 1
staff_degree$is_none <- as.numeric(staff_degree$is_none)
staff_degree$school_code <- sapply(staff_degree$school_code, leadgr, 3)
staff_degree$lea_code <- sapply(staff_degree$lea_code, leadgr, 3)
staff_degree$school_name <- toupper(staff_degree$school_name)
staff_degree$lea_name <- toupper(staff_degree$lea_name)
staff_degree$lea_name[which(staff_degree$lea_code!='001')] <- "PUBLIC CHARTER SCHOOLS"
staff_degree$lea_code[which(staff_degree$lea_code!='001')] <- "000"


sector_degree <- data.frame()
for(a in unique(staff_degree$lea_code)){
	degree_hqt <- subset(staff_degree,lea_code==a)
	lea_code <- a
	lea_name <- degree_hqt$lea_name[1]

	for(b in unique(degree_hqt$year)){
	 	tmp <- subset(degree_hqt,year==b)
		year <- b
		
		total <- nrow(tmp)
		associates <- sum(tmp$is_aa)
		bachelors <- sum(tmp$is_bachelors)
		masters <- sum(tmp$is_masters)
		phd <- sum(tmp$is_phd)
		none <- sum(tmp$is_none)

new_row <- c(year,lea_code,lea_name,associates,bachelors,masters,phd,none,total)
sector_degree <- rbind(sector_degree,new_row)
	}
}
colnames(sector_degree) <- c("year","lea_code","lea_name","num_associates","num_bachelors","num_masters","num_phd","num_none","num_total")

sqlSave(dbrepcard_prod, sector_degree, tablename = "staff_degree_sector_exhibit", append = FALSE, rownames=FALSE)