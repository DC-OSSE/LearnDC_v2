setwd("U:/LearnDC ETL V2/HQT Classes Exhibit/Aggregators")
source("U:/R/tomkit.R")
source("./imports/subproc.R")

school_hqt <- sqlQuery(dbrepcard_prod,"select * from dbo.hqt_classes_school_exhibit_bk")
school_hqt$lea_code <- ifelse(school_hqt$lea_code!='1','000','001')
school_hqt$lea_name <- ifelse(school_hqt$lea_code!='001','CHARTER SECTOR LEA','DISTRICT OF COLUMBIA PUBLIC SCHOOLS')

subgroup_list <- c('All','LOW','HIGH','MIDDLE','ELEM','SEC')

sector_hqt_df <- data.frame()
	for(h in unique(school_hqt$lea_code)){
		sector_hqt <- subset(school_hqt, lea_code == h)
		lea_code <- h
		lea_name <- sector_hqt$lea_name[1]

		for(i in unique(sector_hqt$year)){
			hqt_year <- subset(sector_hqt, year == i)
			year <- i

			for(j in subgroup_list){
				tmp <- subproc(hqt_year, j)
				school_category <- j

				total_classes <- sum(tmp$num_total_classes)
				hq_classes <- sum(tmp$num_hq_classes)
				nhq_classes <- sum(tmp$num_nhq_classes)

				new_row <- cbind(lea_code,lea_name,year,school_category,total_classes,hq_classes,nhq_classes)
								
				sector_hqt_df <- rbind(sector_hqt_df, new_row)
		}
	}
}
colnames(sector_hqt_df) <- c("lea_code","lea_name","year","school_category","num_total_classes","num_hq_classes","num_nhq_classes")

sqlSave(dbrepcard_prod, sector_hqt_df, tablename = "hqt_classes_sector_exhibit_bk", append = FALSE, rownames=FALSE)