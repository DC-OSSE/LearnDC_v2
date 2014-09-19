setwd("U:/LearnDC ETL V2/Graduation Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

grads <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[graduation] WHERE [cohort_status] = 1")




subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","LEP","Economy")




lea_subgroups_df <- data.frame()
for(h in unique(grads$lea_code)){
	.lea_grads <- subset(grads, lea_code == h)

	.lea_code <- h
	.lea_name <- .lea_grads$lea_name[1]

	for(i in unique(.lea_grads$cohort_year)){
		.grads_year <- subset(.lea_grads, cohort_year == i)
		.year <- i + 4

		for(j in subgroups_list){

			.tmp <- subproc(.grads_year, j)
			.subgroup <- j
			.graduates <- sum(.tmp$graduated, na.rm=TRUE)
			.cohort_size <- nrow(.tmp)

			new_row <- c(.lea_code, .lea_name,.subgroup, .year, .graduates, .cohort_size)
							
			lea_subgroups_df <- rbind(lea_subgroups_df, new_row)
		}
	}
}

colnames(lea_subgroups_df) <- c("lea_code","lea_name","subgroup","year","graduates","cohort_size")


sqlSave(dbrepcard_prod, lea_subgroups_df, tablename = "graduation_lea_exhibit", append = FALSE, rownames=FALSE)