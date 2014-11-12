setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(dplyr)


new_eq <- read.csv("./Equity Report Data from Tembo/Fourth Iteration/equityReports_temboMetrics_10 Nov 2014.csv")

new_move <- read.csv("./Equity Report Data from Tembo/Fourth Iteration/Tembo_StudentMovementResults_2014_ 7 Nov 2014.csv")


new_move <- subset(new_move, School_Code %notin% c(175, 861, 137, 480, 104, 462))



equity_prior <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[equity_report_prelim]")
equity_prior$Description <- NULL


new_eq$Month <- ""


new_eq <- select(new_eq, Key, School_Code, School_Year, Student_Group, Metric, SchoolScore, AverageScore, Month,ReportType, NSize)

new_move$count <- NULL
equity_final <- rbind(new_eq, new_move, equity_prior)
equity_final <- subset(equity_final, ReportType == "External")


mgp <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[mgp_longitudinal] WHERE test_year = '2014'")
mgp$Key <- ""
mgp$Month <- ""
mgp$ReportType <- "External"


mgp1 <- mgp
mgp2 <- mgp
mgp1$mgp_2yr <- NULL
mgp2$mgp_1yr <- NULL

mgp1$Metric <- NA
mgp2$Metric <- NA
mgp1$Metric[which(mgp1$subject == "Reading")] <- "CAS Reading Growth"
mgp2$Metric[which(mgp2$subject == "Reading")] <- "CAS Reading 2-year Growth"
mgp1$Metric[which(mgp1$subject == "Math")] <- "CAS Math Growth"
mgp2$Metric[which(mgp2$subject == "Math")] <- "CAS Math 2-year Growth"

colnames(mgp1)[11] <- "SchoolScore"
colnames(mgp2)[11] <- "SchoolScore"

mgp <- rbind(mgp1, mgp2)

mgp$subgroup[which(mgp$subgroup == "All Students")]<- 'All'
mgp$subgroup[which(mgp$subgroup == "FARMS")] <- 'Economy'


## mgp for gender
sgp <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[sgp_longitudinal] WHERE [test_year] = '2014'")

cas <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[assessment] WHERE [year] = '2014'" )

sgp$fay <- 0
sgp$fay[which(sgp$usi %in% cas$usi[which(cas$full_academic_year == 'School')])] <- 1

sgp <- subset(sgp, fay == 1)

tab_gender <- sgp %.%
	group_by(lea_code, lea_name, school_code, school_name, gender) %.%
	summarize(
		NSize = length(usi),
		math_mgp = median(math_sgp, na.rm=TRUE),
		read_mgp = median(read_sgp, na.rm=TRUE))

gender_mgp1 <- select(tab_gender, lea_code, lea_name, school_code, school_name, gender, math_mgp, NSize)
gender_mgp2 <- select(tab_gender, lea_code, lea_name, school_code, school_name, gender, read_mgp, NSize)
colnames(gender_mgp1)[6] <- "SchoolScore"
colnames(gender_mgp2)[6] <- "SchoolScore"

gender_mgp1$Metric <- "CAS Math Growth"
gender_mgp2$Metric <- "CAS Reading Growth"

gender_mgp <- rbind(gender_mgp1, gender_mgp2)

gender_mgp$ReportType <- "External"
gender_mgp$Key  <- ""
gender_mgp$Month  <- ""
gender_mgp$school_year  <- "sy2013-2014"
gender_mgp$ea_year  <- 2013
gender_mgp$test_year  <- 2014

colnames(gender_mgp)[5] <- "Student_Group"
gender_mgp$Student_Group[which(gender_mgp$Student_Group == "F")] <- "FEMALE" 
gender_mgp$Student_Group[which(gender_mgp$Student_Group == "M")] <- "MALE" 


state_tab_gender <- tab_gender <- sgp %.%
	group_by(gender) %.%
	summarize(
		NSize = length(usi),
		math_mgp = median(math_sgp, na.rm=TRUE),
		read_mgp = median(read_sgp, na.rm=TRUE))


gender_mgp$AverageScore <- NA
gender_mgp$AverageScore[which(gender_mgp$Metric == "CAS Math Growth" & gender_mgp$Student_Group == "MALE")] <- 48
gender_mgp$AverageScore[which(gender_mgp$Metric == "CAS Reading Growth" & gender_mgp$Student_Group == "MALE")] <- 47 
gender_mgp$AverageScore[which(gender_mgp$Metric == "CAS Math Growth" & gender_mgp$Student_Group == "FEMALE")] <- 52
gender_mgp$AverageScore[which(gender_mgp$Metric == "CAS Reading Growth" & gender_mgp$Student_Group == "FEMALE")] <- 53


gender_mgp <- gender_mgp[,c("Key", "school_code", "test_year", "Student_Group", "Metric", "SchoolScore", "AverageScore", "Month", "ReportType", "NSize")]

colnames(gender_mgp) <- colnames(equity_final)
## end gender mgp prepare


mgp_state <- read.csv('./Equity Report Data from Tembo/statewide_mgp.csv')
mgp_state <- subset(mgp_state, School_Year == "2013-14")

mgp_state$Student_Group[mgp_state$Student_Group == 'Asian'] <- 'AS7'
mgp_state$Student_Group[mgp_state$Student_Group == 'American Indian / Alaskan Native'] <- 'AM7'
mgp_state$Student_Group[mgp_state$Student_Group == 'Black'] <- 'BL7'
mgp_state$Student_Group[mgp_state$Student_Group == 'Hispanic'] <- 'HI7'
mgp_state$Student_Group[mgp_state$Student_Group == 'Multiracial'] <- 'MU7'
mgp_state$Student_Group[mgp_state$Student_Group == 'White'] <- 'WH7'
mgp_state$Student_Group[mgp_state$Student_Group == 'Free or Reduced Lunch'] <- 'Economy'
mgp_state$Student_Group[mgp_state$Student_Group == 'Limited English Proficiency'] <- 'LEP'
mgp_state$Student_Group[mgp_state$Student_Group == 'Special Education'] <- 'SPED'



mgp_m <- merge(mgp, mgp_state, by.x = c("subgroup","subject","Metric"), by.y= c("Student_Group","subject","Metric"), all.x=TRUE)


mgp_m <- select(mgp_m, Key, school_code, test_year, subgroup, Metric, SchoolScore, AverageScore, Month, ReportType, group_fay_size)
colnames(mgp_m) <- colnames(equity_final)





mgp_m$AverageScore[which(mgp_m$Student_Group == "All")] <- 50
mgp_m <- subset(mgp_m, Student_Group %notin% c("Not-FARMS","Not-LEP","Not-SPED","Grade 4","Grade 5","Grade 6","Grade 7","Grade 8","Grade 10"))



equity_final <- rbind(equity_final, mgp_m, gender_mgp)


equity_final$Student_Group[which(equity_final$Student_Group == "All Students")] <- "All"
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


equity_final$Key[which(equity_final$Key == "")] <- paste(equity_final$School_Code[which(equity_final$Key == "")],
	equity_final$School_Year[which(equity_final$Key == "")], 
	equity_final$Student_Group[which(equity_final$Key == "")], 
	equity_final$Metric[which(equity_final$Key == "")], 
	equity_final$ReportType[which(equity_final$Key == "")], 
	sep = "-")

equity_final <- arrange(equity_final, School_Year, Metric, School_Code)

sqlSave(dbrepcard_prod, equity_final, tablename = "equity_longitudinal_2", rownames=FALSE)


