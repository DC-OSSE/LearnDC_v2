source("U:/R/tomkit.R")
library(dplyr)


mgp_state <- read.csv('U:/LearnDC ETL V2/Equity Report Exhibit/Aggregators/Equity Report Data from Tembo/statewide_mgp.csv')
mgp_state$CAS <- NULL

mgp_state$Student_Group[mgp_state$Student_Group == 'Asian'] <- 'AS7'
mgp_state$Student_Group[mgp_state$Student_Group == 'American Indian / Alaskan Native'] <- 'AM7'
mgp_state$Student_Group[mgp_state$Student_Group == 'Black'] <- 'BL7'
mgp_state$Student_Group[mgp_state$Student_Group == 'Hispanic'] <- 'HI7'
mgp_state$Student_Group[mgp_state$Student_Group == 'Multiracial'] <- 'MU7'
mgp_state$Student_Group[mgp_state$Student_Group == 'White'] <- 'WH7'
mgp_state$Student_Group[mgp_state$Student_Group == 'Free or Reduced Lunch'] <- 'Economy'
mgp_state$Student_Group[mgp_state$Student_Group == 'Limited English Proficiency'] <- 'LEP'
mgp_state$Student_Group[mgp_state$Student_Group == 'Special Education'] <- 'SPED'
mgp_state$Student_Group[mgp_state$Student_Group == 'Female'] <- 'FEMALE'
mgp_state$Student_Group[mgp_state$Student_Group == 'Male'] <- 'MALE'
mgp_state$Student_Group[mgp_state$Student_Group == 'Pacific Islander'] <- 'PI7'



mgp_state$School_Year[which(mgp_state$School_Yea == "2012-13")] <- "2013"
mgp_state$School_Year[which(mgp_state$School_Yea == "2013-14")] <- "2014"

mgp_state$Metric[which(mgp_state$Metric == "CAS Reading Growth")] <- "mgp_1yr"
mgp_state$Metric[which(mgp_state$Metric == "CAS Math Growth")] <- "mgp_1yr"
mgp_state$Metric[which(mgp_state$Metric == "CAS Reading 2-year Growth")] <- "mgp_2yr"
mgp_state$Metric[which(mgp_state$Metric == "CAS Math 2-year Growth")] <- "mgp_2yr"


mgp_state1 <- subset(mgp_state, Metric == "mgp_1yr")
mgp_state2 <- subset(mgp_state, Metric == "mgp_2yr")


colnames(mgp_state1)[5] <- "mgp_1yr"
colnames(mgp_state2)[5] <- "mgp_2yr"


mgp_state1$Metric <- NULL
mgp_state2$Metric <- NULL
mgp_state2$NSize <- NULL


mgp_state <- merge(mgp_state1, mgp_state2, by = c("Student_Group","School_Year","subject"), all.x=TRUE)


colnames(mgp_state) <- c("subgroup","year","subject","NSize","mgp_1yr","mgp_2yr")


df_all <- c("subgroup","year","subject","NSize","mgp_1yr","mgp_2yr")

df_all <-  as.data.frame(rbind(c("All","2014","Math",NA,50,50)
	,c("All","2014","Reading",NA,50,50)
	,c("All","2013","Math",NA,50,50)
	,c("All","2013","Reading",NA,50,50)
	,c("All","2012","Math",NA,50,50)
	,c("All","2012","Reading",NA,50,50)	
	,c("All","2011","Math",NA,50,50)
	,c("All","2011","Reading",NA,50,50)))
colnames(df_all) <- c("subgroup","year","subject","NSize","mgp_1yr","mgp_2yr")



mgp_state <- rbind(mgp_state, df_all)

sqlSave(dbrepcard, mgp_state, tablename = "mgp_state_longitudinal", rownames=FALSE)