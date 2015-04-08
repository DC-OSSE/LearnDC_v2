setwd("U:/LearnDC ETL V2/HQT Classes Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

school_hqt <- sqlQuery(dbrepcard_prod,"select * from dbo.hqt_classes_school_exhibit")


school_hqt$school_code <- sapply(school_hqt$school_code, leadgr, 3)
school_hqt$lea_code <- sapply(school_hqt$lea_code, leadgr, 3)

##7000 lea_code and lea_name "State Level Reporting LEA" for records with school_code == 480 (Incarcerated Youth Program, Correctional)

# school_hqt$lea_code[which(school_hqt$school_code=='0480')] <- '4001'
# school_hqt$lea_name[which(school_hqt$school_code=='0480')] <- 'STATE-LEVEL REPORTING LEA'

lea_hqt_df <- data.frame()
	for(h in unique(school_hqt$lea_code)){
		.lea_hqt <- subset(school_hqt, lea_code == h)
		.lea_code <- h
		.lea_name <- .lea_hqt$lea_name[1]

		for(i in unique(.lea_hqt$year)){
			.hqt_year <- subset(.lea_hqt, year == i)
			.year <- i

			for(j in unique(.hqt_year$school_category)){
				.hqt_cat <- subset(.hqt_year,school_category==j)
				.school_category <- j

				for(k in unique(.hqt_cat$poverty_quartile)){
					.tmp <- subset(.hqt_cat,poverty_quartile==k)
					.poverty_quartile <- k

				.total_classes <- sum(.tmp$num_total_classes)
				.hq_classes <- sum(.tmp$num_hq_classes)
				.nhq_classes <- sum(.tmp$num_nhq_classes)

				new_row <- cbind(.lea_code,.lea_name,.year,.school_category,.poverty_quartile,.total_classes,.hq_classes,.nhq_classes)
								
				lea_hqt_df <- rbind(lea_hqt_df, new_row)
			}
		}
	}
}

colnames(lea_hqt_df) <- c("lea_code","lea_name","year","school_category","poverty_quartile","num_total_classes","num_hq_classes","num_nhq_classes")


sqlSave(dbrepcard_prod, lea_hqt_df, tablename = "hqt_classes_lea_exhibit", append = FALSE, rownames=FALSE)