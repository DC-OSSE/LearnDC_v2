setwd("U:/LearnDC ETL V2/MGP Exhibit/Aggregators")
source("U:/R/tomkit.R")
source("./imports/subproc.R")
library(dplyr)

sgp <- sqlFetch(dbrepcard,"dbo.sgp_longitudinal")
math_sgp <- select(sgp,test_year,lea_code,lea_name,school_code,school_name,usi,race=race_ethnicity,gender,fay,exclude,economy=farms,ell_prog=ell,ell_2yr,special_ed=sped,sped_2yr,new_to_us,grade,scale_score=math_scale_score,scale_score_prior1=math_scale_score_prior1,year_prior1=math_year_prior1,scale_score_prior2=math_scale_score_prior2,year_prior2=math_year_prior2,sgp=math_sgp)
math_sgp$subject <- 'Math'
read_sgp <- select(sgp,test_year,lea_code,lea_name,school_code,school_name,usi,race=race_ethnicity,gender,fay,exclude,economy=farms,ell_prog=ell,ell_2yr,special_ed=sped,sped_2yr,new_to_us,grade,scale_score=read_scale_score,scale_score_prior1=read_scale_score_prior1,year_prior1=read_year_prior1,scale_score_prior2=read_scale_score_prior2,year_prior2=read_year_prior2,sgp=read_sgp)
read_sgp$subject <- 'Reading'

sgp_all <- rbind(math_sgp,read_sgp)
sgp_all$school_code <- sapply(sgp_all$school_code, leadgr, 3)
sgp_all$lea_code <- sapply(sgp_all$lea_code, leadgr, 3)
sgp_all$gender[which(sgp_all$gender=='M')] <- 'MALE'
sgp_all$gender[which(sgp_all$gender=='F')] <- 'FEMALE'
sgp_all$economy <- toupper(sgp_all$economy)
sgp_all$ell_prog <- toupper(sgp_all$ell_prog)
sgp_all$ell_2yr <- toupper(sgp_all$ell_2yr)
sgp_all$special_ed <- toupper(sgp_all$special_ed)
sgp_all$sped_2yr <- toupper(sgp_all$sped_2yr)

sgp_all <- subset(sgp_all,fay %in% c('S'))

subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","LEP","Economy")

school_subgroups_df <- data.frame()
for(a in unique(sgp_all$school_code)){
	school_sgp <- subset(sgp_all,school_code==a)
	lea_code <- school_sgp$lea_code[1]
	lea_name <- school_sgp$lea_name[1]
	school_code <- a
	school_name <- school_sgp$school_name[1]

		for(b in unique(school_sgp$test_year)){
			sgp_year <- subset(school_sgp,test_year==b)
			year <- b

			for(c in unique(sgp_year$subject)){
				sgp_subject <- subset(sgp_year,subject==c)
				subject <- c

				for(d in subgroups_list){
					tmp <- subproc(sgp_subject,d)
					subgroup <- d

					group_fay_size <- nrow(tmp)
					mgp_1yr <- median(tmp$sgp,na.rm=TRUE)

		new_row <- c(lea_code,lea_name,school_code,school_name,subgroup,year,group_fay_size,subject,mgp_1yr)	
		school_subgroups_df <- rbind(school_subgroups_df,new_row)
			
			}
		}
	}
}
colnames(school_subgroups_df) <- c("lea_code","lea_name","school_code","school_name","subgroup","year","group_fay_size","subject","mgp_1yr")

school_subgroups_df$group_fay_size <- as.numeric(school_subgroups_df$group_fay_size)
school_subgroups_df$mgp_1yr <- round(as.numeric(school_subgroups_df$mgp_1yr),1)
school_subgroups_df$year <- as.numeric(school_subgroups_df$year)

school_subgroups_df_prior <- school_subgroups_df
colnames(school_subgroups_df_prior) <- paste0("prior_",colnames(school_subgroups_df_prior))
school_subgroups_df_prior$prior_year <- school_subgroups_df$year + 1
school_mgp <- merge(school_subgroups_df, school_subgroups_df_prior, by.x = c("year","school_code","school_name","subgroup","subject"), by.y = c("prior_year","prior_school_code","prior_school_name","prior_subgroup","prior_subject"), all.x=TRUE)
school_mgp$mgp_2yr <- round((((school_mgp$mgp_1yr*school_mgp$group_fay_size)+(school_mgp$prior_mgp_1yr*school_mgp$prior_group_fay_size))/(school_mgp$group_fay_size+school_mgp$prior_group_fay_size)),1)

school_mgp$flag <- 0
school_mgp$flag[which(school_mgp$group_fay_size<=25|school_mgp$prior_group_fay_size<=25)] <- 1
school_mgp$mgp_2yr[which(school_mgp$flag==1)] <- NA

school_mgp <- select(school_mgp,year,lea_code,lea_name,school_code,school_name,subgroup,subject,group_fay_size,mgp_1yr,mgp_2yr)

sqlSave(dbworking, school_mgp, tablename = "mgp_school_exhibit_draft", append = FALSE, rownames=FALSE)