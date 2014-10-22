source("U:/R/tomkit.R")






eq_state <- sqlQuery(dbrepcard_prod, "SELECT DISTINCT [School_Year],[Student_Group],[Metric], [AverageScore]
  FROM [dbo].[equity_longitudinal] WHERE [metric] in ('CAS Math Growth','CAS Math 2-year Growth','CAS Reading Growth','CAS Reading 2-year Growth')")





eq <- sqlQuery(dbrepcard_prod, "SELECT DISTINCT * 
  FROM [dbo].[equity_longitudinal] WHERE [metric] in ('CAS Math Growth','CAS Reading Growth')")


rep <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[mgp_longitudinal]")







rep[which(rep$school_code == 181 & rep$ea_year == "2013" & rep$subgroup == "BL7" & rep$subject == "Math"),]
rep[which(rep$school_code == 1120 & rep$ea_year == "2012" & rep$subgroup == "FARMS" & rep$subject == "Math"),]
rep[which(rep$school_code == 196 & rep$ea_year == "2012" & rep$subgroup == "SPED" & rep$subject == "Reading"),]






