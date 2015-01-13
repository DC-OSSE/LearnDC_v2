source("U:/R/tomkit.R")
setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Aggregators/New Dual Purpose Enrollment Tables")

enr <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[enrollment_school_exhibit]")
cachedEnr <- enr

pcsb_demo <- read.csv("pcsb_demo_insert_11-20-2014.csv")
pcsb_demo$subgroup[which(pcsb_demo$subgroup == "Female")] <- "FEMALE"
pcsb_demo$subgroup[which(pcsb_demo$subgroup == "Male")] <- "MALE"


for(i in unique(pcsb_demo$school_Code)){
	.tmp <- subset(pcsb_demo, school_Code == i)

	for(j in unique(.tmp$subgroup)){

		new_number <- .tmp$Pct[which(
						.tmp$school_Code == i &
						.tmp$subgroup == j)] *
					enr$enrollment[which(
						enr$year == 2013 &
						enr$grade == "All" &
						enr$subgroup == "All" &
						enr$school_code == i)]

		old <- enr$enrollment[which(
						enr$year == 2013 &
						enr$grade == "All" &
						enr$subgroup == j &
						enr$school_code == i)]

		enr$enrollment[which(
						enr$year == 2013 &
						enr$grade == "All" &
						enr$subgroup == j &
						enr$school_code == i)] <- new_number
	}
}

sqlSave(dbrepcard_prod, enr, tablename = "enrollment_school_exhibit_pcsb_alterations", rownames = FALSE)


# enr <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[enrollment_school_exhibit]")
# enr <- subset(enr, year == 2013)
# enr <- subset(enr, grade == "All")
# for (i in unique(enr$school_code)) {
# 	total_sped <- enr$enrollment[which(enr$subgroup == "SPED" & enr$school_code==i)]
# 	lvl1 <- enr$enrollment[which(enr$subgroup == "SPED Level 1" & enr$school_code==i)]
# 	lvl2 <- enr$enrollment[which(enr$subgroup == "SPED Level 2" & enr$school_code==i)]
# 	lvl3 <- enr$enrollment[which(enr$subgroup == "SPED Level 3" & enr$school_code==i)]
# 	lvl4 <- enr$enrollment[which(enr$subgroup == "SPED Level 4" & enr$school_code==i)]
# 	sum <- sum(lvl1, lvl2, lvl3, lvl4)

# 	print(paste0('code: ', i, ', sped: ', total_sped, ', in levels: ', sum))
# }





enr_option2 <- subset(enr, year == 2013)
enr_option2 <- subset(enr_option2, subgroup == "All" | grade == "All")

option2_final <- data.frame()
for(i in unique(enr_option2$school_code)){
    .tmp <- subset(enr_option2, school_code == i)

    for(j in unique(.tmp$grade)){
        .tmp2 <- subset(.tmp, grade == j)

        if(j == "All"){
            # sped_denom <- .tmp2$enrollment[which(.tmp2$subgroup == "SPED")]
            sped_denom <- cachedEnr$enrollment[which(cachedEnr$year == 2013 & cachedEnr$school_code==i & cachedEnr$grade=="All" & cachedEnr$subgroup=="SPED")]
            for(k in unique(.tmp2$subgroup)){

                if (k %in% c('SPED Level 1', 'SPED Level 2', 'SPED Level 3', 'SPED Level 4')) {

                    .tmp2$enrollment[which(
                        .tmp2$subgroup == k)] <- 
                        .tmp2$enrollment[which(.tmp2$subgroup == k)]  / sped_denom

                } else if (k !="All") {
                    .tmp2$enrollment[which(
                        .tmp2$subgroup == k)] <- 
                        .tmp2$enrollment[which(.tmp2$subgroup == k)]  / 
                        .tmp2$enrollment[which(.tmp2$subgroup == "All")]
                }
            }
        }
        option2_final <- rbind(option2_final, .tmp2 )
    }
}



option2_final$enrollment <- round(option2_final$enrollment,3)

sqlSave(dbrepcard_prod, option2_final, tablename = "enrollment_school_exhibit_mixed_values", rownames = FALSE)





