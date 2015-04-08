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
sgp_all$special_ed <- toupper(sgp_all$special_ed)

sgp_all <- subset(sgp_all,fay %in% c('S','C','D'))

subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","LEP","Economy")

state_subgroups_df <- data.frame()
		for(b in unique(sgp_all$test_year)){
			sgp_year <- subset(sgp_all,test_year==b)
			year <- b

			for(c in unique(sgp_year$subject)){
				sgp_subject <- subset(sgp_year,subject==c)
				subject <- c

				for(d in subgroups_list){
					tmp <- subproc(sgp_subject,d)
					subgroup <- d

					group_fay_size <- length(tmp$usi)
					mgp_1yr <- median(tmp$sgp,na.rm=TRUE)

		new_row <- c(subgroup,year,group_fay_size,subject,mgp_1yr)	
		state_subgroups_df <- rbind(state_subgroups_df,new_row)
			
			}
		}
	}

colnames(state_subgroups_df) <- c("subgroup","year","group_fay_size","subject","mgp_1yr")

state_subgroups_df$group_fay_size <- as.numeric(state_subgroups_df$group_fay_size)
state_subgroups_df$mgp_1yr <- round(as.numeric(state_subgroups_df$mgp_1yr),2)
state_subgroups_df$year <- as.numeric(state_subgroups_df$year)

state_subgroups_df_prior <- state_subgroups_df
colnames(state_subgroups_df_prior) <- paste0("prior_",colnames(state_subgroups_df_prior))
state_subgroups_df_prior$prior_year <- state_subgroups_df$year + 1
state_subgroups_df <- merge(state_subgroups_df, state_subgroups_df_prior, by.x = c("year","subgroup","subject"), by.y = c("prior_year","prior_subgroup","prior_subject"), all.x=TRUE)
state_subgroups_df$mgp_2yr <- round((((state_subgroups_df$mgp_1yr*state_subgroups_df$group_fay_size)+(state_subgroups_df$prior_mgp_1yr*state_subgroups_df$prior_group_fay_size))/(state_subgroups_df$group_fay_size+state_subgroups_df$prior_group_fay_size)),2)

state_subgroups_df$flag <- 0
state_subgroups_df$flag[which(state_subgroups_df$group_fay_size<=25|state_subgroups_df$prior_group_fay_size<=25)] <- 1
state_subgroups_df$mgp_2yr[which(state_subgroups_df$flag==1)] <- NA

state_subgroups_df <- select(state_subgroups_df,year,subgroup,subject,group_fay_size,mgp_1yr,mgp_2yr)

sqlSave(dbworking, state_subgroups_df, tablename = "mgp_state_exhibit_draft", append = FALSE, rownames=FALSE)