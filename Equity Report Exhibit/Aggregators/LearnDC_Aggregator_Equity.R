source("U:/R/tomkit.R")
library(dplyr)
library(reshape2)

setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Aggregators/Equity Report Data from Tembo/Fifth Iteration")

## LOAD PRIOR DATA TO USE AS TEMPLATE
equity_prior <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[equity_report_prelim] WHERE ReportType = 'External' and
	[Metric] not in ('CAS Math Growth','CAS Reading Growth','CAS Math 2-year Growth','CAS Reading 2-year Growth','CAS Math Proficiency','CAS Reading Proficiency')")
equity_prior$Description <- NULL



exp_isa <- read.csv("BothSector_ExpulsionMetrics - DCPS_ ISA_TotalSuspensions UPDATED 11.20.2014.csv")
exp_isa <- exp_isa[,1:11]
exp_isa <- subset(exp_isa, !is.na(exp_isa$School_Code))
exp_isa$LEA.Code <- NULL
exp_isa$school.name <- NULL
exp_isa$Month <- ""
exp_isa <- select(exp_isa, Key, School_Code, School_Year, Student_Group, Metric, SchoolScore, AverageScore, Month, ReportType, NSize)


## FIXING DUPLICATES IN THE EXPULSION DATA
exp_isa$SchoolScore[which(exp_isa$School_Code == "1110" & exp_isa$Student_Group == "All Students" & exp_isa$Metric == "Expulsion Rate")] <- 0.23
exp_isa$AverageScore[which(exp_isa$School_Code == "1110" & exp_isa$Student_Group == "All Students" & exp_isa$Metric == "Expulsion Rate")] <- 0.02

exp_isa$SchoolScore[which(exp_isa$School_Code == "1110" & exp_isa$Student_Group == "All Students" & exp_isa$Metric == "Expulsions")] <- 1
exp_isa$AverageScore[which(exp_isa$School_Code == "1110" & exp_isa$Student_Group == "All Students" & exp_isa$Metric == "Expulsions")] <- 6

exp_isa$SchoolScore[which(exp_isa$School_Code == "137" & exp_isa$Student_Group == "All Students" & exp_isa$Metric == "Expulsion Rate")] <- 0.67
exp_isa$AverageScore[which(exp_isa$School_Code == "137" & exp_isa$Student_Group == "All Students" & exp_isa$Metric == "Expulsion Rate")] <- 0.06

exp_isa$SchoolScore[which(exp_isa$School_Code == "137" & exp_isa$Student_Group == "All Students" & exp_isa$Metric == "Expulsions")] <- 1
exp_isa$AverageScore[which(exp_isa$School_Code == "137" & exp_isa$Student_Group == "All Students" & exp_isa$Metric == "Expulsions")] <- 3


exp_isa <- unique(exp_isa)
## END FIXING DUPLICATES IN THE EXPULSION DATA




susp_counts <- read.csv("BothSectors-SuspensionRates_1plus_11plus.csv")
susp_counts$Month <- ""
susp_counts <- select(susp_counts, Key, School_Code, School_Year, Student_Group, Metric, SchoolScore, AverageScore, Month, ReportType, NSize)




dcps_myew <- read.csv("DCPS MYE MYW data_tembo_updated_2014_november 19.csv")
dcps_myew <- select(dcps_myew, Key, School_Code, School_Year, Student_Group, Metric, SchoolScore, AverageScore, Month, ReportType, NSize)

dir <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[schooldir_sy1314]")
dcps_myew <- subset(dcps_myew, School_Code %notin% dir$school_code[which(dir$lea_code != 1)])


pcs_mye <- read.csv("PCS MYE data.csv")
pcs_myw <- read.csv("PCS MYW data.csv")

colnames(pcs_mye) <- paste0("entry_",colnames(pcs_mye))
colnames(pcs_myw) <- paste0("withd_",colnames(pcs_myw))

pcs_myew <- merge(pcs_mye, pcs_myw, by.x = c("entry_school_code","entry_entry_month"), by.y= c("withd_school_code","withd_exit_month"))


pcs_myew$entry_School.Name <- NULL
pcs_myew$withd_School.Name <- NULL
pcs_myew$withd_enrollment <- NULL

pcs_myew$NetCumulative <- pcs_myew$entry_Pct.Entry - pcs_myew$withd_pct_exits



pcs_myew_melt <- melt(pcs_myew,  id.vars = c("entry_school_code", "entry_entry_month","entry_enrollment"))
pcs_myew_melt <- subset(pcs_myew_melt, variable %in% c("entry_Pct.Entry","withd_pct_exits","NetCumulative"))

colnames(pcs_myew_melt) <- c("School_Code","Month","NSize","Metric","SchoolScore")

pcs_myew_melt$Metric <- as.character(pcs_myew_melt$Metric)
pcs_myew_melt$Metric[which(pcs_myew_melt$Metric == "entry_Pct.Entry")] <- "Entry"
pcs_myew_melt$Metric[which(pcs_myew_melt$Metric == "withd_pct_exits")] <- "Withdrawal"
pcs_myew_melt$Metric[which(pcs_myew_melt$Metric == "NetCumulative")] <- "Net Cumulative"

pcs_myew_melt$Student_Group <- "All Students"
pcs_myew_melt$School_Year <- "2013-14"
pcs_myew_melt$ReportType <- "External"
pcs_myew_melt$AverageScore <- NA

pcs_myew_melt$Key <- paste(pcs_myew_melt$School_Code, pcs_myew_melt$School_Year, pcs_myew_melt$Student_Group, pcs_myew_melt$Metric, pcs_myew_melt$ReportType)

pcs_myew_melt <- select(pcs_myew_melt, Key, School_Code, School_Year, Student_Group, Metric, SchoolScore, AverageScore, Month, ReportType, NSize)





pcs_total_susp <- read.csv("PCS Total Suspensions.csv")
pcs_total_susp$SCHOOL_NAME <- NULL

colnames(pcs_total_susp) <- c("School_Code","SchoolScore")
pcs_total_susp$Student_Group <- "All Students"
pcs_total_susp$School_Year <- "2013-14"
pcs_total_susp$ReportType <- "External"
pcs_total_susp$AverageScore <- NA
pcs_total_susp$Metric <- "Total Suspensions"
pcs_total_susp$Month <- ""
pcs_total_susp$NSize <- NA
pcs_total_susp$Key <- paste(pcs_total_susp$School_Code, pcs_total_susp$School_Year, pcs_total_susp$Student_Group, pcs_total_susp$Metric, pcs_total_susp$ReportType)
pcs_total_susp <- select(pcs_total_susp, Key, School_Code, School_Year, Student_Group, Metric, SchoolScore, AverageScore, Month, ReportType, NSize)




pcs_isa <- read.csv("PCS All students ISA campus pct.csv")
pcs_isa$SCHOOL_NAME <- NULL
pcs_isa$ISA.Adult.Only <- NULL
pcs_isa <- subset(pcs_isa, !is.na(ISA..entire.campus.PK..12.))
colnames(pcs_isa) <- c("School_Code","SchoolScore")
pcs_isa$Student_Group <- "All Students"
pcs_isa$School_Year <- "2013-14"
pcs_isa$ReportType <- "External"
pcs_isa$AverageScore <- NA
pcs_isa$Metric <- "In-Seat Attendance Rate"
pcs_isa$Month <- ""
pcs_isa$NSize <- NA
pcs_isa$Key <- paste(pcs_isa$School_Code, pcs_isa$School_Year, pcs_isa$Student_Group, pcs_isa$Metric, pcs_isa$ReportType)
pcs_isa <- select(pcs_isa, Key, School_Code, School_Year, Student_Group, Metric, SchoolScore, AverageScore, Month, ReportType, NSize)



## ONLY USING THIS FILE FOR PCS ISA SUBGROUPS, WHICH WERE MISSING FROM OTHER FILES
pcs_isa_subgroups <- read.csv("Tembo Attendance and Discipline File plus PCSB changes to all student campus ISA rates and expulsion rates.csv")
pcs_isa_subgroups <- subset(pcs_isa_subgroups, Metric == "In-Seat Attendance Rate")
pcs_isa_subgroups <- subset(pcs_isa_subgroups, Student_Group != "All Students")
pcs_isa_subgroups <- subset(pcs_isa_subgroups, School_Code %in% pcs_isa$School_Code)
pcs_isa_subgroups$Month <- ""
pcs_isa_subgroups <- select(pcs_isa_subgroups, Key, School_Code, School_Year, Student_Group, Metric, SchoolScore, AverageScore, Month, ReportType, NSize)

briya_adult <- read.csv("PCS All students ISA campus pct.csv")
briya_adult <- subset(briya_adult,School.Code==126)
briya_adult$SCHOOL_NAME <- NULL
briya_adult$ISA..entire.campus.PK..12. <- NULL
briya_adult <- subset(briya_adult, !is.na(ISA..entire.campus.PK..12.))
colnames(briya_adult) <- c("School_Code","SchoolScore")
briya_adult$Student_Group <- "Adult Only"
briya_adult$School_Year <- "2013-14"
briya_adult$ReportType <- "External"
briya_adult$AverageScore <- NA
briya_adult$Metric <- "In-Seat Attendance Rate"
briya_adult$Month <- ""
briya_adult$NSize <- NA
briya_adult$Key <- paste(briya_adult$School_Code, briya_adult$School_Year, briya_adult$Student_Group, briya_adult$Metric, briya_adult$ReportType)
briya_adult <- select(briya_adult, Key, School_Code, School_Year, Student_Group, Metric, SchoolScore, AverageScore, Month, ReportType, NSize)









equity_final <- rbind(equity_prior, exp_isa, susp_counts, dcps_myew, pcs_myew_melt, pcs_total_susp, pcs_isa, pcs_isa_subgroups,briya_adult)

equity_final$Student_Group[which(equity_final$Student_Group == "All Students")] <- "All"
equity_final$Student_Group[which(equity_final$Student_Group == "Adult Only")] <- "AO"
equity_final$Student_Group[which(equity_final$Student_Group == "Asian")] <- "AS7"
equity_final$Student_Group[which(equity_final$Student_Group == "Black non-Hispanic")] <- "BL7"
equity_final$Student_Group[which(equity_final$Student_Group == "Hispanic / Latino")] <- "HI7"
equity_final$Student_Group[which(equity_final$Student_Group == "Multiracial")] <- "MU7"
equity_final$Student_Group[which(equity_final$Student_Group == "White non-Hispanic")] <- "WH7"
equity_final$Student_Group[which(equity_final$Student_Group == "Female")] <- "FEMALE"
equity_final$Student_Group[which(equity_final$Student_Group == "Male")] <- "MALE"
equity_final$Student_Group[which(equity_final$Student_Group %in% c("Free or Reduced Lunch","Free or Reduced Price Lunch"))] <- "Economy"
equity_final$Student_Group[which(equity_final$Student_Group == "Limited English Proficiency")] <- "LEP"
equity_final$Student_Group[which(equity_final$Student_Group == "Special Education")] <- "SPED"


equity_final$School_Year[which(equity_final$School_Year == "2011-12")] <- "2012"
equity_final$School_Year[which(equity_final$School_Year == "2012-13")] <- "2013"
equity_final$School_Year[which(equity_final$School_Year == "2013-14")] <- "2014"


equity_final$Month[which(equity_final$Month == "January")] <- 1
equity_final$Month[which(equity_final$Month == "February")] <- 2
equity_final$Month[which(equity_final$Month == "March")] <- 3
equity_final$Month[which(equity_final$Month == "April")] <- 4
equity_final$Month[which(equity_final$Month == "May")] <- 5
equity_final$Month[which(equity_final$Month == "October")] <- 10
equity_final$Month[which(equity_final$Month == "November")] <- 11
equity_final$Month[which(equity_final$Month == "December")] <- 12
equity_final$Month <- as.numeric(equity_final$Month)


equity_final <- arrange(equity_final, School_Year, Metric, School_Code)


sqlSave(dbrepcard_prod, equity_final, tablename = "equity_longitudinal2", rownames=FALSE)