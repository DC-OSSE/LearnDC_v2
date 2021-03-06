setwd("U:/LearnDC ETL V2/Graduation Exhibit/Aggregators")

source("U:/R/tomkit.R")
source("./imports/subproc.R")


grads <- sqlQuery(dbrepcard, "SELECT * FROM dbo.graduation where cohort_status=1")


subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","LEP","Economy")

state_subgroups_df <- data.frame()
for(h in c("Four Year ACGR","Five Year ACGR")){
	.type <- h

	for(i in unique(grads$cohort_year)){
		
		.grads_year <- subset(grads, cohort_year == i)
		
		if(.type == "Four Year ACGR"){
			.year <- i + 4
		}
		else if (.type == "Five Year ACGR"){	
			.year <- i + 5
		}

		for(j in subgroups_list){

			.tmp <- subproc(.grads_year, j)
			
			.subgroup <- j

			if(.type == "Four Year ACGR"){
				.graduates <- sum(.tmp$graduated_4yr, na.rm=TRUE)
			}
			else if (.type == "Five Year ACGR"){	
				.graduates <- sum(.tmp$graduated_5yr, na.rm=TRUE)
				if(.graduates == 0){.graduates <- NA}
			}

			.cohort_size <- nrow(.tmp)

			new_row <- c(.subgroup, .year, .type, .graduates, .cohort_size)
							
			state_subgroups_df <- rbind(state_subgroups_df, new_row)
		}
	}
}


colnames(state_subgroups_df) <- c("subgroup","year","type","graduates","cohort_size")
state_subgroups_df <- subset(state_subgroups_df,!is.na(graduates))
state_subgroups_df <- subset(state_subgroups_df,year==max(year,na.rm=TRUE))

sqlQuery(dbrepcard_prod,sprintf("delete from dbo.graduation_state_exhibit WHERE year = '%s'",max(state_subgroups_df$year,na.rm=TRUE)))
sqlSave(dbrepcard_prod,state_subgroups_df,tablename="graduation_state_exhibit",append=TRUE,rownames=FALSE)