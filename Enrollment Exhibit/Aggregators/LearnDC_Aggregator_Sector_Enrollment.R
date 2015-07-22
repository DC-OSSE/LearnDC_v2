setwd("U:/LearnDC ETL V2/Enrollment Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

## Load Data
enr <- sqlFetch(dbrepcard,"dbo.enrollment_w2015_pkcbo")
## Change Hispanic Coding
enr$race[which(enr$ethnicity == "YES")] <- "HI7"
## Remove DYRS & 2014 Hospitality from Sectoral Calculations
# enr <- subset(enr,lea_code %notin% c(4001,4002))
enr <- enr[which(enr$lea_code!='122' | enr$ea_year!=2014),]
enr <- subset(enr,lea_code %notin% 6000)

enr$lea_code <- sapply(enr$lea_code,leadgr,3)
enr$grade <- sapply(enr$grade,leadgr,2)
enr$lea_code[which(enr$lea_code!='001')] <- '000'
enr$lea_name[which(enr$lea_code!='001')] <- 'PUBLIC CHARTER SCHOOLS'

subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","SPED Level 1","SPED Level 2","SPED Level 3","SPED Level 4","LEP","Economy","Direct Cert")

sector_subgroups_df <- data.frame()
for(h in unique(enr$lea_code)){
	sector_enr <- subset(enr, lea_code == h)

	lea_code <- h
	lea_name <- sector_enr$lea_name[1]

	for(i in unique(sector_enr$ea_year)){
		enr_year <- subset(sector_enr, ea_year == i)
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
							

				sector_subgroups_df <- rbind(sector_subgroups_df, new_row)

			}
		}
	}	
}	
colnames(sector_subgroups_df) <- c("lea_code","lea_name","year","subgroup","grade","enrollment")



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
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "13")] <- "grade 13"
sector_subgroups_df$grade[which(sector_subgroups_df$grade == "AO")] <- "grade AO"


sqlSave(dbrepcard_prod,sector_subgroups_df,tablename="enrollment_sector_exhibit_w2015",append=FALSE,rownames=FALSE)