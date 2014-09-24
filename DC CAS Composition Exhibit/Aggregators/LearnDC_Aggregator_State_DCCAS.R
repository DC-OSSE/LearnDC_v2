setwd("U:/LearnDC ETL V2/DC CAS Composition Exhibit/Aggregators")
source("./imports/subproc.R")
source("U:/R/tomkit.R")

cas <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[assm_comp]") 


subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","LEP","Economy","Direct Cert")



cas <- subset(cas, lea_code %notin% c(4002, 5000))

state_subgroups_df <- data.frame()


for(h in unique(cas$year)){
	tmp <- subset(cas, year == h)
	.year <- h

	for(j in subgroups_list){
		tmp_2 <- subproc(tmp, j)
		# print(paste0("   Subgroup: ",j," ## Rows :",nrow(tmp_2)))

		.subgroup <- j

		for(k in 0:length(unique(tmp_2$tested_grade))){
			if(k == 0){
				tmp_3 <- tmp_2
				.grade <- "All"
			} else{
				.grade <- unique(tmp_2$tested_grade)[k]
				tmp_3 <- subset(tmp_2, tested_grade == .grade)
			}
			# print(paste0("      Grade: ", .grade, " ## Rows: ", nrow(tmp_3)))


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
								

				new_row <- c(.year,
					.subgroup, .grade, .subject, .enrollment_status,.n_eligible, .n_test_takers,
					.n_proficient_advanced, .n_below_basic, .n_basic, .n_proficient, .n_advanced
					)

				state_subgroups_df <- rbind(state_subgroups_df, new_row)
			}			
		}
	}
}


colnames(state_subgroups_df) <- c("year",
						"subgroup", "grade", "subject", ".enrollment_status", "n_eligible", "n_test_takers",
						"proficient_or_advanced", "below_basic", "basic", "proficient", "advanced")


state_subgroups_df$grade[which(state_subgroups_df$grade == 3)] <- "grade 3"
state_subgroups_df$grade[which(state_subgroups_df$grade == 4)] <- "grade 4"
state_subgroups_df$grade[which(state_subgroups_df$grade == 5)] <- "grade 5"
state_subgroups_df$grade[which(state_subgroups_df$grade == 6)] <- "grade 6"
state_subgroups_df$grade[which(state_subgroups_df$grade == 7)] <- "grade 7"
state_subgroups_df$grade[which(state_subgroups_df$grade == 8)] <- "grade 8"
state_subgroups_df$grade[which(state_subgroups_df$grade == 10)] <- "grade 10"


sqlSave(dbrepcard_prod, state_subgroups_df, tablename = "cas_state_exhibit", append = FALSE, rownames=FALSE)