setwd("U:/LearnDC ETL V2/Equity Report Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(dplyr)

equity <- read.csv("./Equity Report Data from Tembo/Second Iteration/equity.csv")
movement <- read.csv("./Equity Report Data from Tembo/Second Iteration/movement.csv")



equity_prior <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[equity_report_prelim]")
equity_prior$Description <- NULL


equity$Month <- ""



## NOTE I ALSO CHANGED SCHOOL YEAR FORMAT TO XXXX-XXXX AND RENAMED SUBGROUPS (TO DC CAS NOMENCLATURE) IN THE SQL DATA 


equity <- select(equity, Key, School_Code, School_Year, Student_Group, Metric, SchoolScore, AverageScore, ReportType, NSize, Month)
equity_final <- rbind(equity, movement, equity_prior)
equity_final <- subset(equity_final, ReportType == "External")



sqlSave(dbrepcard_prod, equity_final, tablename = "equity_longitudinal", rownames=FALSE)