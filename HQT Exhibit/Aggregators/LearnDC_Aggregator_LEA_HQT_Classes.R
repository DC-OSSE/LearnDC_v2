setwd("U:/LearnDC ETL V2/HQT Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

school_hqt <- sqlQuery(dbrepcard_prod,"select * from dbo.hqt_school_exhibit_w2014")

school_hqt$school_code <- sapply(school_hqt$school_code, leadgr, 4)
school_hqt$lea_code <- sapply(school_hqt$lea_code, leadgr, 4)

##7000 lea_code and lea_name "State Level Reporting LEA" for records with school_code == 480 (Incarcerated Youth Program, Correctional)

# school_hqt$lea_code[which(school_hqt$school_code=='0480')] <- '4001'
# school_hqt$lea_name[which(school_hqt$school_code=='0480')] <- 'State-Level Reporting LEA'

subgroups_list <- c("All","LOW","HIGH","NEITHER","ELEM","SEC")
  
lea_hqt_df <- data.frame()
	for(h in unique(school_hqt$lea_code)){
		.lea_hqt <- subset(school_hqt, lea_code == h)
		.lea_code <- h
		.lea_name <- .lea_hqt$lea_name[1]

		for(i in unique(.lea_hqt$year)){
			.hqt_year <- subset(.lea_hqt, year == i)
			.year <- i

			for(j in subgroups_list){
				
				.tmp <- subproc(.hqt_year,j)
				.subgroup <- j

				.total_classes <- sum(.tmp$num_total_classes)
				.hq_classes <- sum(.tmp$num_hq_classes)
				.nhq_classes <- sum(.tmp$num_nhq_classes)

				new_row <- c(.lea_code,.lea_name,.year,.subgroup,.total_classes,.hq_classes,.nhq_classes)
								
				lea_hqt_df <- rbind(lea_hqt_df, new_row)
			}
		}
	}

colnames(lea_hqt_df) <- c("lea_code","lea_name","year","school_entity","num_total_classes","num_hq_classes","num_nhq_classes")


sqlSave(dbrepcard_prod, lea_hqt_df, tablename = "hqt_classes_lea_exhibit_w2014", append = FALSE, rownames=FALSE)