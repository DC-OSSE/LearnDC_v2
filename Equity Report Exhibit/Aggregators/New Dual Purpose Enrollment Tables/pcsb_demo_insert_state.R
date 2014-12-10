source("U:/R/tomkit.R")
library(dplyr)

setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Aggregators/New Dual Purpose Enrollment Tables")


pcsb_demo <- read.csv("pcsb_demo_insert_11-20-2014.csv")
pcsb_demo$subgroup[which(pcsb_demo$subgroup == "Female")] <- "FEMALE"
pcsb_demo$subgroup[which(pcsb_demo$subgroup == "Male")] <- "MALE"


pcsb_agg <- pcsb_demo %>%
	group_by(subgroup) %>%
	summarize(enrollment=sum(pcsb_enroll))

pcsb_agg$sector <- "PCS"

dcps_enr <- sqlQuery(dbrepcard_prod, "SELECT subgroup, enrollment FROM [dbo].[enrollment_lea_exhibit] WHERE lea_code = 1 and year = '2013' and grade = 'All'")
dcps_enr <- subset(dcps_enr, subgroup %in% pcsb_agg$subgroup)

dcps_enr$sector <- "DCPS"

both_sector <- rbind(dcps_enr, pcsb_agg)


state_agg <- both_sector %>% 
	group_by(subgroup) %>%
	summarize(enrollment = sum(enrollment))


state_agg$total_agg_enroll <- 41827 + 41086
state_agg$year <- 2013
state_agg$grade <- "All"
state_agg$Pct <- state_agg$enrollment/state_agg$total_agg_enroll
state_agg$enrollment <- state_agg$Pct
state_agg <- subset(state_agg, select=c(year, grade, subgroup, enrollment))
state_all <- read.csv("1_line.csv")
state_agg <- rbind(state_agg, state_all)

enr <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[enrollment_state_exhibit]")
enr <- subset(enr, (year==2013 & subgroup=='All' & grade!='All') | (year==2013 & grade=='All' & subgroup %in% c('SPED Level 1', 'SPED Level 2','SPED Level 3', 'SPED Level 4')))
enr$enrollment[which(enr$subgroup %in% c('SPED Level 1', 'SPED Level 2','SPED Level 3', 'SPED Level 4'))] <- (enr$enrollment/11108)

state_agg <- rbind(state_agg, enr)

sqlSave(dbrepcard_prod, state_agg, tablename = "enrollment_state_exhibit_pcsb_alterations", rownames = FALSE)
