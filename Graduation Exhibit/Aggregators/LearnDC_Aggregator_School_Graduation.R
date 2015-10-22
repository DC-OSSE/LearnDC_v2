setwd("U:/LearnDC ETL V2/Graduation Exhibit/Aggregators")
source("U:/R/tomkit.R")
source("./imports/subproc.R")
library(dplyr)
subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","LEP","Economy")
##APPEND/UNION NEW 4-YEAR GRADUATION FILE AND UPDATE 5-YEAR GRADUATION FILE FROM PREVIOUS YEAR'S DATA WHEN AVAILABLE
grads <- sqlQuery(dbrepcard, "SELECT * FROM dbo.graduation where cohort_status=1")
grads$grade <- "09"
grads$grad_year <- grads$cohort_year + 4
grads$school_code <- sapply(grads$school_code, leadgr, 4)

dir <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[school_mapping_sy1415]") %>% mutate(school_code=sapply(school_code,leadgr,4),grade=sapply(grade,leadgr,2),grad_year=ea_year+1) %>% arrange(ea_year,school_code) %>% filter(grade=="09") %>% select(-ea_year)

grads <- merge(grads,dir,by=c("grad_year","grade","school_code"),all.x=TRUE)
# grads[which(is.na(grads$sy1415_school_code) & grads$grad_year=='2015'),]$sy1415_school_code <- grads[which(is.na(grads$sy1415_school_code)),]$school_code
# grads[which(is.na(grads$sy1415_school_name) & grads$grad_year=='2015'),]$sy1415_school_name <- grads[which(is.na(grads$sy1415_school_name)),]$school_name
grads <- subset(grads, sy1415_school_name != "NULL")
grads_lim <- subset(grads, cohort_status == 1)

school_subgroups_df <- data.frame()
for(g in c("Four Year ACGR","Five Year ACGR")){
	.type <- g

	for(h in unique(grads_lim$sy1415_school_code)){
		.school_grads <- subset(grads_lim, sy1415_school_code == h)

		.lea_code <- .school_grads$lea_code[1]
		.lea_name <- .school_grads$lea_name[1]
		.school_code <- h
		.school_name <- .school_grads$sy1415_school_name[1]

		for(i in unique(.school_grads$cohort_year)){
			.grads_year <- subset(.school_grads, cohort_year == i)

			if(.type == "Four Year ACGR"){
				.year <- i + 4
			}
			else if (.type == "Five Year ACGR"){	
				.year <- i + 5
			}


			for(j in subgroups_list){

				.tmp <- subproc(.grads_year, j)
				.subgroup <- j

				if(.type == "Four Year ACGR"){
					.graduates <- sum(.tmp$graduated_4yr, na.rm=TRUE)
				}
				else if (.type == "Five Year ACGR"){	
					.graduates <- sum(.tmp$graduated_5yr, na.rm=TRUE)
					if(.graduates == 0){.graduates <- NA}
				}

				.cohort_size <- nrow(.tmp)

				new_row <- c(.lea_code, .lea_name, .school_code, .school_name, .subgroup, .year, .type, .graduates, .cohort_size)
								
				school_subgroups_df <- rbind(school_subgroups_df, new_row)
			}
		}
	}
}
colnames(school_subgroups_df) <- c("lea_code","lea_name","school_code","school_name","subgroup","year","type","graduates","cohort_size")
school_subgroups_df <- subset(school_subgroups_df,!is.na(graduates))
school_subgroups_df <- subset(school_subgroups_df,year==max(year,na.rm=TRUE))

##DROPS AND REWRITES WHOLE TABLE
# sqlDrop(dbrepcard_prod,"graduation_school_exhibit_w2014")
# sqlSave(dbrepcard_prod, school_subgroups_df, tablename = "graduation_school_exhibit_w2014", append = FALSE, rownames=FALSE)
##APPENDS RESULTS
sqlQuery(dbrepcard_prod,sprintf("delete from dbo.graduation_school_exhibit WHERE year = '%s'",max(school_subgroups_df$year,na.rm=TRUE)))
sqlSave(dbrepcard_prod,school_subgroups_df,tablename="graduation_school_exhibit",append=TRUE,rownames=FALSE)