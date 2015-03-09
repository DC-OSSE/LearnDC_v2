setwd("U:/LearnDC ETL V2/HQT Classes Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

school_hqt <- sqlQuery(dbrepcard_prod,"select * from dbo.hqt_classes_school_exhibit_w2014")

school_hqt$school_code <- sapply(school_hqt$school_code, leadgr, 4)
school_hqt$lea_code <- sapply(school_hqt$lea_code, leadgr, 4)

subgroups_list <- c("All","LOW","HIGH","MIDDLE","ELEM","SEC")

state_hqt_df <- data.frame()
			for(i in unique(school_hqt$year)){
			.hqt_year <- subset(school_hqt, year == i)
			.year <- i

			for(j in subgroups_list){
				
				.tmp <- subproc(.hqt_year,j)
				.subgroup <- j

				.total_classes <- sum(.tmp$num_total_classes)
				.hq_classes <- sum(.tmp$num_hq_classes)
				.nhq_classes <- sum(.tmp$num_nhq_classes)

				new_row <- c(.year,.subgroup,.total_classes,.hq_classes,.nhq_classes)
								
				state_hqt_df <- rbind(state_hqt_df, new_row)
			}
		}

colnames(state_hqt_df) <- c("year","school_entity","num_total_classes","num_hq_classes","num_nhq_classes")


sqlSave(dbrepcard_prod, state_hqt_df, tablename = "hqt_classes_state_exhibit_w2014", append = FALSE, rownames=FALSE)