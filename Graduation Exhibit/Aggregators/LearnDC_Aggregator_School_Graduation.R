setwd("U:/LearnDC ETL V2/Graduation Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

grads <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[graduation_w2014_5yr] WHERE [cohort_status] = 1 and [lea_code] not in ('2','3')")


dir <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[school_mapping_sy1314]")

grads$grade <- "9"
grads$year <- grads$cohort_year + 2

grads <- merge(grads, dir, by.x = c("year","grade","school_code"), by.y= c("ea_year","grade","school_code"), all.x=TRUE)



grads$sy1314_school_code[which(grads$school_name == "CAPITAL CITY UPPER SCHOOL")] <- "1207"
grads$sy1314_school_name[which(grads$school_name == "CAPITAL CITY UPPER SCHOOL")] <- "CAPITAL CITY HIGH SCHOOL PCS"

grads$sy1314_school_code[which(grads$school_name == "Cesar Chavez Parkside HS")] <- "109"
grads$sy1314_school_name[which(grads$school_name == "Cesar Chavez Parkside HS")] <- "Cesar Chavez PCS for Public Policy-Parkside HS"
grads$sy1314_school_code[which(grads$school_name == "BALLOU STAY")] <- "462"
grads$sy1314_school_name[which(grads$school_name == "BALLOU STAY")] <- "BALLOU STAY"


grads <- subset(grads, sy1314_school_name != "NULL")



subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","LEP","Economy")


grads_lim <- subset(grads, cohort_status == 1)

school_subgroups_df <- data.frame()
for(h in unique(grads_lim$sy1314_school_code)){
	.school_grads <- subset(grads_lim, sy1314_school_code == h)

	.lea_code <- .school_grads$lea_code[1]
	.lea_name <- .school_grads$lea_name[1]
	.school_code <- h
	.school_name <- .school_grads$sy1314_school_name[1]

	for(i in unique(.school_grads$cohort_year)){
		.grads_year <- subset(.school_grads, cohort_year == i)
		.year <- i + 4

		for(j in subgroups_list){

			.tmp <- subproc(.grads_year, j)
			.subgroup <- j
			.graduates <- sum(.tmp$graduated, na.rm=TRUE)
			.graduates_5yr <- sum(.tmp$graduated_5yr, na.rm=TRUE)
			if(.graduates_5yr == 0){.graduates_5yr <- "null"}

			.cohort_size <- nrow(.tmp)

			new_row <- c(.lea_code, .lea_name, .school_code, .school_name, .subgroup, .year, .graduates, .graduates_5yr, .cohort_size)
							
			school_subgroups_df <- rbind(school_subgroups_df, new_row)
		}
	}
}

colnames(school_subgroups_df) <- c("lea_code","lea_name","school_code","school_name","subgroup","year","graduates","graduates_5yr","cohort_size")


sqlSave(dbrepcard_prod, school_subgroups_df, tablename = "graduation_school_exhibit_2014", append = FALSE, rownames=FALSE)