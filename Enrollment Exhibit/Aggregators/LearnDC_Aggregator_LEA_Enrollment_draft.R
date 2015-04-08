setwd("U:/LearnDC ETL V2/Enrollment Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

## Load Data
enr <- sqlFetch(dbworking,"dbo.draft_enr_audit_all_1415")
## Change Hispanic Coding
enr$race[which(enr$ethnicity == "YES")] <- "HI7"
## Remove DYRS

enr$lea_code <- sapply(enr$lea_code,leadgr,3)
enr$grade <- sapply(enr$grade,leadgr,2)

subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","SPED Level 1","SPED Level 2","SPED Level 3","SPED Level 4","LEP","Economy","Direct Cert")

lea_subgroups_df <- data.frame()
for(h in unique(enr$lea_code)){
	lea_enr <- subset(enr, lea_code == h)

	lea_code <- h
	lea_name <- lea_enr$lea_name[1]

	for(i in unique(lea_enr$ea_year)){
		enr_year <- subset(lea_enr, ea_year == i)
		year <- i

		for(j in subgroups_list){

			tmp <- subproc(enr_year, j)
			subgroup <- j

			for(k in 0:length(unique(tmp$grade))){
				if(k == 0){
					tmp_g <- tmp
					.grade <- "All"
				} else{
					.grade <- unique(tmp$grade)[k]
					tmp_g <- subset(tmp, grade == .grade)
				}

				enrollment <- nrow(tmp_g)

				new_row <- c(lea_code, lea_name, year, subgroup, .grade, enrollment)
							

				lea_subgroups_df <- rbind(lea_subgroups_df, new_row)

			}
		}
	}	
}	
colnames(lea_subgroups_df) <- c("lea_code","lea_name","year","subgroup","grade","enrollment")


lea_subgroups_df$grade[which(lea_subgroups_df$grade == "01")] <- "grade 1"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "02")] <- "grade 2"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "03")] <- "grade 3"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "04")] <- "grade 4"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "05")] <- "grade 5"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "06")] <- "grade 6"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "07")] <- "grade 7"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "08")] <- "grade 8"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "09")] <- "grade 9"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "10")] <- "grade 10"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "11")] <- "grade 11"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "12")] <- "grade 12"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "13")] <- "grade 13"
lea_subgroups_df$grade[which(lea_subgroups_df$grade == "AO")] <- "grade AO"


sqlSave(dbrepcard_prod, lea_subgroups_df, tablename = "enrollment_lea_exhibit_w2015_draft", append = FALSE, rownames=FALSE)