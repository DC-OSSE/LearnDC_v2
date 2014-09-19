setwd("U:/LearnDC ETL V2/Graduation Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")


grads <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[graduation] WHERE [cohort_status] = 1")



subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","LEP","Economy")



state_subgroups_df <- data.frame()



for(i in unique(grads$cohort_year)){
	.grads_year <- subset(grads, cohort_year == i)
	.year <- i + 4

	for(j in subgroups_list){

		.tmp <- subproc(.grads_year, j)
		.subgroup <- j
		.graduates <- sum(.tmp$graduated, na.rm=TRUE)
		.cohort_size <- nrow(.tmp)

		new_row <- c(.subgroup, .year, .graduates, .cohort_size)
						
		state_subgroups_df <- rbind(state_subgroups_df, new_row)
	}
}


colnames(state_subgroups_df) <- c("subgroup","year","graduates","cohort_size")


sqlSave(dbrepcard_prod, state_subgroups_df, tablename = "graduation_state_exhibit", append = FALSE, rownames=FALSE)