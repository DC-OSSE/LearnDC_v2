setwd("U:/LearnDC ETL V2/Graduation Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

grads <- sqlQuery(dbrepcard, "SELECT * FROM dbo.graduation where cohort_status=1")

grads <- subset(grads,lea_code %notin% c('4002','NULL','6000','9000'))

grads$lea_code[which(grads$lea_code!='001')] <- "000"
grads$lea_name[which(grads$lea_code!='001')] <- 'PUBLIC CHARTER SCHOOLS'
grads$lea_name <- toupper(grads$lea_name)

subgroups_list <- c("All","MALE","FEMALE","AM7","AS7","BL7","HI7","MU7","PI7","WH7","SPED","LEP","Economy")

charter_subgroups_df <- data.frame()
for(g in c("Four Year ACGR","Five Year ACGR")){
	for(h in unique(grads$lea_code)){
		.charter_grads <- subset(grads, lea_code == h)

		.type <- g
		.lea_code <- h
		.lea_name <- .charter_grads$lea_name[1]

		for(i in unique(.charter_grads$cohort_year)){
			.grads_year <- subset(.charter_grads, cohort_year == i)

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

				new_row <- c(.lea_code, .lea_name,.subgroup, .year,.type, .graduates, .cohort_size)
								
				charter_subgroups_df <- rbind(charter_subgroups_df, new_row)
			}
		}
	}
}

colnames(charter_subgroups_df) <- c("lea_code","lea_name","subgroup","year","type","graduates","cohort_size")
charter_subgroups_df <- subset(charter_subgroups_df,!is.na(graduates))
charter_subgroups_df <- subset(charter_subgroups_df,year==max(year,na.rm=TRUE))

sqlQuery(dbrepcard_prod,sprintf("delete from dbo.graduation_sector_exhibit WHERE year = '%s'",max(charter_subgroups_df$year,na.rm=TRUE)))
sqlSave(dbrepcard_prod,charter_subgroups_df,tablename="graduation_sector_exhibit",append=TRUE,rownames=FALSE)