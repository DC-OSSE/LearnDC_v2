source("U:/R/tomkit.R")
library(dplyr)
library(reshape2)

equity_long <- sqlQuery(dbrepcard_prod,"select * from reportcard_production.dbo.equity_longitudinal")

tembo_add <- sqlQuery(dbworking,"select * from working.dbo.tembo_equity_report_dcps_missing_2014")

equity_long_tembo_add <- rbind(equity_long,tembo_add)

add <- read.csv("X:/Equity Reports/2013-2014 SY/FINAL DATA/KW Updates post final/briya_isa_adults_only_addition.csv")

equity_long_tembo_add <- subset(equity_long_tembo_add, Key %notin% c("126-2013-14-Adults Only-In-Sea"))

equity_long_tembo_delta <- rbind(equity_long_tembo_add,add)

add104_168 <- read.csv("X:/Equity Reports/FINAL DATA/KW Updates post final/ISA 104 and 168.csv")
equity_longitudinal3 <- rbind(equity_long_tembo_delta,add104_168)