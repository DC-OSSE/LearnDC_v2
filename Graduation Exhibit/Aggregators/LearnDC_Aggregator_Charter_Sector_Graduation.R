setwd("U:/LearnDC ETL V2/Graduation Exhibit/Aggregators")

source("U:/R/tomkit.R")

source("./imports/subproc.R")

grads <- sqlQuery(dbrepcard, "SELECT * FROM dbo.graduation_w2014 where cohort_status=1")

grads$lea_code[which(grads$school_code=='0480' & grads$cohort_year==2010)] <- '4001'
grads$lea_name[which(grads$school_code=='0480' & grads$cohort_year==2010)] <- 'State-Level Reporting LEA'
##7000 lea_code and lea_name "State Level Reporting LEA" for records with school_code == 480 (Incarcerated Youth Program, Correctional for cohort_year==2010)

grads <- subset(grads,lea_code %notin% c('4002','NULL'))

grads$lea_code[which(grads$lea_code!='0001')] <- 0
grads$lea_name[which(grads$lea_code!='0001')] <- 'Charter Sector LEA'
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


sqlSave(dbrepcard_prod, charter_subgroups_df, tablename = "graduation_sector_exhibit_w2014", append = FALSE, rownames=FALSE)