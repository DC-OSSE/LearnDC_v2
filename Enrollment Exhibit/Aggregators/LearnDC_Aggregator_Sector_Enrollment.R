setwd("U:/LearnDC ETL V2/Enrollment Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

## Load Data
enr <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[enrollment]") 
## Change Hispanic Coding
enr$race[which(enr$ethnicity == "YES")] <- "HI7"
## Remove DYRS
enr <- subset(enr,lea_code %notin% c(4001))
enr <- subset(enr,lea_code != '0122' & ea_year==2014) 


enr$lea_code[which(enr$lea_code!='1')] <- 0
enr$lea_name[which(enr$lea_code!='1')] <- 'CHARTER SECTOR LEA'
enr$lea_code <- sapply(enr$lea_code,leadgr,4)

subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","SPED Level 1","SPED Level 2","SPED Level 3","SPED Level 4","LEP","Economy","Direct Cert")


sector_subgroups_df <- data.frame()

for(h in unique(enr$lea_code)){
	sector_enr <- subset(enr, lea_code == h)

	.lea_code <- h
	.lea_name <- sector_enr$lea_name[1]

	for(i in unique(sector_enr$ea_year)){
		.enr_year <- subset(sector_enr, ea_year == i)
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

				new_row <- c(.lea_code, .lea_name, .year, .subgroup, .grade, .enrollment)
							

				sector_subgroups_df <- rbind(sector_subgroups_df, new_row)

			}
		}
	}	
}	
colnames(sector_subgroups_df) <- c("lea_code","lea_name","year","subgroup","grade","enrollment")


sector_subgroups_df$grade[which(sector_subgroups_df$grade == "PS")] <- "grade PK3"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "PK")] <- "grade PK4"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "KG")] <- "grade KG"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "01")] <- "grade 1"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "02")] <- "grade 2"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "03")] <- "grade 3"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "04")] <- "grade 4"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "05")] <- "grade 5"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "06")] <- "grade 6"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "07")] <- "grade 7"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "08")] <- "grade 8"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "09")] <- "grade 9"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "10")] <- "grade 10"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "11")] <- "grade 11"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "12")] <- "grade 12"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "AO")] <- "grade AO"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "UN")] <- "ungraded"


sqlSave(dbrepcard_prod, sector_subgroups_df, tablename = "enrollment_sector_exhibit", append = FALSE, rownames=FALSE)