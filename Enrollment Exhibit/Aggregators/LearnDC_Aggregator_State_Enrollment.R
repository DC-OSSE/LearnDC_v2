setwd("U:/LearnDC ETL V2/Enrollment Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

## Load Data
enr <- sqlFetch(dbrepcard,"dbo.enrollment")
# enr <- sqlFetch(dbrepcard,"dbo.enrollment_w2015_pkcbo")
## Change Hispanic Coding
enr$race[which(enr$ethnicity == "YES")] <- "HI7"
## Remove DYRS
# enr <- subset(enr, lea_code %notin% c(4001,4002))
enr$grade <- sapply(enr$grade,leadgr,2)

subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","SPED Level 1","SPED Level 2","SPED Level 3","SPED Level 4","LEP","Economy","Direct Cert")

state_subgroups_df <- data.frame()

for(i in unique(enr$ea_year)){
	enr_year <- subset(enr, ea_year == i)
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

			new_row <- c(year, subgroup, .grade, enrollment)
						
			state_subgroups_df <- rbind(state_subgroups_df, new_row)

		}
	}
}
colnames(state_subgroups_df) <- c("year","subgroup","grade","enrollment")


state_subgroups_df$grade[which(state_subgroups_df$grade == "01")] <- "grade 1"
state_subgroups_df$grade[which(state_subgroups_df$grade == "02")] <- "grade 2"
state_subgroups_df$grade[which(state_subgroups_df$grade == "03")] <- "grade 3"
state_subgroups_df$grade[which(state_subgroups_df$grade == "04")] <- "grade 4"
state_subgroups_df$grade[which(state_subgroups_df$grade == "05")] <- "grade 5"
state_subgroups_df$grade[which(state_subgroups_df$grade == "06")] <- "grade 6"
state_subgroups_df$grade[which(state_subgroups_df$grade == "07")] <- "grade 7"
state_subgroups_df$grade[which(state_subgroups_df$grade == "08")] <- "grade 8"
state_subgroups_df$grade[which(state_subgroups_df$grade == "09")] <- "grade 9"
state_subgroups_df$grade[which(state_subgroups_df$grade == "10")] <- "grade 10"
state_subgroups_df$grade[which(state_subgroups_df$grade == "11")] <- "grade 11"
state_subgroups_df$grade[which(state_subgroups_df$grade == "12")] <- "grade 12"
state_subgroups_df$grade[which(state_subgroups_df$grade == "AO")] <- "grade AO"


sqlSave(dbrepcard_prod,state_subgroups_df,tablename="enrollment_state_exhibit",append=FALSE,rownames=FALSE)