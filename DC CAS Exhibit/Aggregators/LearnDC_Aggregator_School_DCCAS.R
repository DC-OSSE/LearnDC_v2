setwd("U:/LearnDC ETL V2/DC CAS Exhibit/JSON ETL")
source("./imports/subproc.R")
source("U:/R/tomkit.R")


cas <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[assessment]")
dir <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[school_mapping_sy1314]")

cas$school_grade[which(cas$school_grade=='AO')] <- 13
cas$school_grade <- as.integer(cas$school_grade)
cas$school_grade[which(cas$school_grade==13)] <- 'AO'


cas <- merge(cas, dir, by.x = c("ea_year","school_grade","school_code"), by.y= c("ea_year","grade","school_code"), all.x=TRUE)

cas$sy1314_school_name <- toupper(cas$sy1314_school_name)
cas$lea_name <- toupper(cas$lea_name)

subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","LEP","Economy","Direct Cert")



school_subgroups_df <- data.frame()
start.time <- Sys.time()
cas_no_inv <- subset(cas, math_invalidation %notin% c("A","M"))
for(g in c(1:2)){

	if(g == 1){
		cas_acct <- subset(cas_no_inv, full_academic_year == "School") 
		.enrollment_status <- "full_year"
	} else if (g==2){
		cas_acct <- cas_no_inv
		.enrollment_status <- "all"
	}

	for(h in unique(cas_acct$year)){
		cas_year <- subset(cas_acct, year == h)
		.year <- h

		for(i in unique(cas_year$sy1314_school_code)){

			tmp <- subset(cas_year, sy1314_school_code == i)

			.lea_code <- tmp$lea_code[1]
			.lea_name <- tmp$lea_name[1] 
			.school_code <- i
			.school_name <- tmp$sy1314_school_name[1]

			for(j in subgroups_list){
				tmp_2 <- subproc(tmp, j)

				.subgroup <- j

				for(k in 0:length(unique(tmp_2$tested_grade))){
					if(k == 0){
						tmp_3 <- tmp_2
						.grade <- "all"
					} else{
						.grade <- unique(tmp_2$tested_grade)[k]
						tmp_3 <- subset(tmp_2, tested_grade == .grade)
					}

					.n_eligible <- nrow(tmp_3)

					for(subject in c("math","read")){
						.subject <- subject

						if(subject == "math"){
							.profs <- tmp_3$math_level[which(tmp_3$math_acct == 1)]
						} else if (subject =="read"){
							.profs <- tmp_3$read_level[which(tmp_3$read_acct == 1)]
						}

					
						.n_test_takers <- length(.profs)
						.n_proficient_advanced <- length(.profs[which(.profs %in% c("Proficient","Advanced"))])
						.n_below_basic <- length(.profs[which(.profs == "Below Basic")])

						.n_basic <- length(.profs[which(.profs == "Basic")])	
						.n_proficient <- length(.profs[which(.profs == "Proficient")])					
						.n_advanced <- length(.profs[which(.profs == "Advanced")])
										

						new_row <- c(.year, .lea_code, .lea_name, .school_code, .school_name, .subgroup, .grade, .subject, .enrollment_status,.n_eligible, .n_test_takers, .n_proficient_advanced, .n_below_basic, .n_basic, .n_proficient, .n_advanced
							)

						school_subgroups_df <- rbind(school_subgroups_df, new_row)
					}			
				}
			}
		}
	}
}
end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken

colnames(school_subgroups_df) <- c("year", "lea_code", "lea_name", "school_code", "school_name","subgroup", "grade", "subject", "enrollment_status", "n_eligible", "n_test_takers","proficient_or_advanced", "below_basic", "basic", "proficient", "advanced")




comp <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[assm_comp]") 
science <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[assm_science]") 


comp <- merge(comp, dir, by.x = c("ea_year","tested_grade","school_code"), by.y= c("ea_year","grade","school_code"), all.x=TRUE)
science <- merge(science, dir, by.x = c("ea_year","tested_grade","school_code"), by.y= c("ea_year","grade","school_code"), all.x=TRUE)



.enrollment_status <- "all"
.subgroup <- "All"
.grade <- "All"
.subject <- "composition"

for(x in unique(comp$year)){
	tmp <- subset(comp, year == x)
	.year <- x

	for(y in unique(tmp$sy1314_school_code)){
		tmp2 <- subset(tmp, sy1314_school_code == y)

		.lea_code <- tmp2$lea_code[1]
		.lea_name <- tmp2$lea_name[1]
		.school_code <- y
		.school_name <-  tmp2$sy1314_school_name[1]

		.profs <- tmp2$comp_level[which(tmp2$comp_empty == 0)]

		.n_eligible <- nrow(tmp2)
		.n_test_takers <- length(.profs)
		.n_proficient_advanced <- length(.profs[which(.profs %in% c("Proficient","Advanced"))])
		.n_below_basic <- length(.profs[which(.profs == "Below Basic")])

		.n_basic <- length(.profs[which(.profs == "Basic")])	
		.n_proficient <- length(.profs[which(.profs == "Proficient")])					
		.n_advanced <- length(.profs[which(.profs == "Advanced")])

		new_row <- c(.year, .lea_code, .lea_name, .school_code, .school_name,
			.subgroup, .grade, .subject, .enrollment_status,.n_eligible, .n_test_takers,
			.n_proficient_advanced, .n_below_basic, .n_basic, .n_proficient, .n_advanced
			)

		school_subgroups_df <- rbind(school_subgroups_df, new_row)
	}
}



.subject <- "science"

for(x in unique(science$year)){
	tmp <- subset(science, year == x)
	.year <- x

	for(y in unique(tmp$sy1314_school_code)){
		tmp2 <- subset(tmp, sy1314_school_code == y)

		.lea_code <- tmp2$lea_code[1]
		.lea_name <- tmp2$lea_name[1]
		.school_code <- y
		.school_name <-  tmp2$sy1314_school_name[1]

		.profs <- tmp2$science_level[which(tmp2$science_empty == 0)]

		.n_eligible <- nrow(tmp2)
		.n_test_takers <- length(.profs)
		.n_proficient_advanced <- length(.profs[which(.profs %in% c("Proficient","Advanced"))])
		.n_below_basic <- length(.profs[which(.profs == "Below Basic")])

		.n_basic <- length(.profs[which(.profs == "Basic")])	
		.n_proficient <- length(.profs[which(.profs == "Proficient")])					
		.n_advanced <- length(.profs[which(.profs == "Advanced")])

		new_row <- c(.year, .lea_code, .lea_name, .school_code, .school_name, .subgroup, .grade, .subject, .enrollment_status,.n_eligible, .n_test_takers, .n_proficient_advanced, .n_below_basic, .n_basic, .n_proficient, .n_advanced)

		school_subgroups_df <- rbind(school_subgroups_df, new_row)
	}
}






school_subgroups_df$grade[which(school_subgroups_df$grade == 3)] <- "grade 3"
school_subgroups_df$grade[which(school_subgroups_df$grade == 4)] <- "grade 4"
school_subgroups_df$grade[which(school_subgroups_df$grade == 5)] <- "grade 5"
school_subgroups_df$grade[which(school_subgroups_df$grade == 6)] <- "grade 6"
school_subgroups_df$grade[which(school_subgroups_df$grade == 7)] <- "grade 7"
school_subgroups_df$grade[which(school_subgroups_df$grade == 8)] <- "grade 8"
school_subgroups_df$grade[which(school_subgroups_df$grade == 10)] <- "grade 10"
school_subgroups_df$grade[which(school_subgroups_df$grade == "All")] <- "all"

school_subgroups_df <- subset(school_subgroups_df, n_eligible > 0)



sqlSave(dbrepcard_prod, school_subgroups_df, tablename = "cas_school_exhibit", append = FALSE)