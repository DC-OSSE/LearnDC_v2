setwd("U:/LearnDC ETL V2/Enrollment Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

## Load Data
enr <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[enrollment]") 
## Change Hispanic Coding
enr$race[which(enr$ethnicity == "YES")] <- "HI7"
## Remove DYRS
enr <- subset(enr, lea_code %notin% c(4001))


dir <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[school_mapping_sy1314]")
dir$school_code <- sapply(dir$school_code, leadgr, 4)
dir$grade <- sapply(dir$grade, leadgr, 2)

enr$grade[which(enr$grade == "PK")] <- "PK4"
enr$grade[which(enr$grade == "PS")] <- "PK3"


enr <- merge(enr, dir, by.x = c("ea_year","grade","school_code"), by.y= c("ea_year","grade","school_code"), all.x=TRUE)
enr$sy1314_school_name <- toupper(enr$sy1314_school_name)



subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","SPED Level 1","SPED Level 2","SPED Level 3","SPED Level 4","LEP","Economy","Direct Cert")




school_subgroups_df <- data.frame()
for(h in unique(enr$sy1314_school_code)){
	school_enr <- subset(enr, sy1314_school_code == h)

	.school_code <- h
	.school_name <- school_enr$sy1314_school_name[1]
	.lea_code <- school_enr$lea_code[1]
	.lea_name <- school_enr$lea_name[1]

	for(i in unique(school_enr$ea_year)){
		.enr_year <- subset(school_enr, ea_year == i)
		.year <- i

		for(j in subgroups_list){

			.tmp <- subproc(.enr_year, j)
			.subgroup <- j

			for(k in 0:length(unique(.tmp$grade))){
				if(k == 0){
					.tmp_g <- .tmp
					.grade <- "All"
				} else{
					.grade <- unique(.tmp$grade)[k]
					.tmp_g <- subset(.tmp, grade == .grade)
				}

				.enrollment <- nrow(.tmp_g)

				new_row <- c(.lea_code, .lea_name, .school_code, .school_name, .year, .subgroup, .grade, .enrollment)
							

				school_subgroups_df <- rbind(school_subgroups_df, new_row)

			}
		}
	}	
}	

colnames(school_subgroups_df) <- c("lea_code","lea_name","school_code","school_name","year","subgroup","grade","enrollment")


school_subgroups_df$grade[which(school_subgroups_df$grade == "PS")] <- "grade PK3"
school_subgroups_df$grade[which(school_subgroups_df$grade == "PK")] <- "grade PK4"
school_subgroups_df$grade[which(school_subgroups_df$grade == "KG")] <- "grade KG"
school_subgroups_df$grade[which(school_subgroups_df$grade == "01")] <- "grade 1"
school_subgroups_df$grade[which(school_subgroups_df$grade == "02")] <- "grade 2"
school_subgroups_df$grade[which(school_subgroups_df$grade == "03")] <- "grade 3"
school_subgroups_df$grade[which(school_subgroups_df$grade == "04")] <- "grade 4"
school_subgroups_df$grade[which(school_subgroups_df$grade == "05")] <- "grade 5"
school_subgroups_df$grade[which(school_subgroups_df$grade == "06")] <- "grade 6"
school_subgroups_df$grade[which(school_subgroups_df$grade == "07")] <- "grade 7"
school_subgroups_df$grade[which(school_subgroups_df$grade == "08")] <- "grade 8"
school_subgroups_df$grade[which(school_subgroups_df$grade == "09")] <- "grade 9"
school_subgroups_df$grade[which(school_subgroups_df$grade == "10")] <- "grade 10"
school_subgroups_df$grade[which(school_subgroups_df$grade == "11")] <- "grade 11"
school_subgroups_df$grade[which(school_subgroups_df$grade == "12")] <- "grade 12"
school_subgroups_df$grade[which(school_subgroups_df$grade == "AO")] <- "grade AO"
school_subgroups_df$grade[which(school_subgroups_df$grade == "UN")] <- "ungraded"


sqlSave(dbrepcard_prod, school_subgroups_df, tablename = "enrollment_school_exhibit", append = FALSE, rownames=FALSE)