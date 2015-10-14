##THIS EXHIBIT IS NOT SHOWING ON LEARNDC.ORG AND THE UNEXCUSED ABSENCES EXHIBIT HAS NOT BEEN UPDATED FOR THE SY1415 EQUITY REPORTS!!!
setwd("X:/Learn DC/State Equity Report Development")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

abs <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Metric], [NSize], [SchoolScore], [AverageScore]
                FROM [dbo].[equity_longitudinal3] WHERE [Metric] in ('Unexcused Absences 1-5','Unexcused Absences 6-10','Unexcused Absences 11-15','Unexcused Absences 16-25','Unexcused Absences > 25')")

abs_long <- melt(abs, id.vars = c("School_Year", "School_Code", "Student_Group", "Metric"))
abs_long$Metric<- paste0(abs_long$Metric, "_",abs_long$variable)
abs_long$variable <- NULL
abs_wide <- dcast(abs_long, School_Year + School_Code + Student_Group ~ Metric, value.var = "value")

colnames(abs_wide) <- c("year","school_code","subgroup","state_more_than_25_days","more_than_25_days_n","more_than_25_days","state_1_5_days","X1_5_days_n","1_5_days","state_11_15_days","X11_15_days_n","11_15_days","state_16_25_days","X16_25_days_n","16_25_days","state_6_10_days","X6_10_days_n","6_10_days")
abs_wide$X1_5_days_n <- NULL
abs_wide$X6_10_days_n <- NULL
abs_wide$X11_15_days_n <- NULL
abs_wide$X16_25_days_n <- NULL
abs_wide$more_than_25_days_n <- NULL

abs_wide <- abs_wide[,c(2,1,3,7,13,9,11,5,6,12,8,10,4)]

abs_wide$school_code <- sapply(abs_wide$school_code, leadgr, 4)

state_vals <- abs_wide[which(abs_wide$school_code=='0161'),]

state_abs_all <- state_vals %>%
    select(year,subgroup,'1-5_days'=state_1_5_days,'6-10_days'=state_6_10_days,
         '11-15_days'=state_11_15_days,'16-25_days'=state_16_25_days,more_than_25_days=state_more_than_25_days,
         state_1_5_days,state_6_10_days,state_11_15_days,state_16_25_days,state_more_than_25_days) %>%
    mutate(population='All',state_1_5_days=NA,state_6_10_days=NA,state_11_15_days=NA,
         state_16_25_days=NA,state_more_than_25_days=NA)

state_abs_gen <- as.data.frame(c(state_abs_all[,c(1,2,8)],round(state_abs_all[,c(-1,-2,-8)]/1.25,0))) %>%
mutate(population='Gen')
names(state_abs_gen) <- gsub("X","",names(state_abs_gen))
names(state_abs_gen) <- gsub("[.]","-",names(state_abs_gen))


state_abs_alt <- as.data.frame(c(state_abs_all[,c(1,2,8)],round(state_abs_all[,c(-1,-2,-8)]/1.75,0))) %>%
mutate(population='Alt')
names(state_abs_alt) <- gsub("X","",names(state_abs_alt))
names(state_abs_alt) <- gsub("[.]","-",names(state_abs_alt))

state_abs <- rbind.data.frame(state_abs_all,state_abs_gen,state_abs_alt)

strtable(state_abs)

key_index <- c(1:2,8)
value_index <- c(3:7,9:13)

setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

nested_list <- lapply(1:nrow(state_abs), FUN = function(i){ 
  list(key = list(state_abs[i,key_index]), 
       val = list(state_abs[i,value_index]))
})

json <- toJSON(nested_list, na="null")
json <- gsub("[[","",json, fixed=TRUE)
json <- gsub("]]","",json, fixed=TRUE)
json <- prettify(json)


newfile <- file("unexcused_absences.json", encoding="UTF-8")
sink(newfile)

cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "state",', sep="", fill=TRUE)
cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
cat('"org_code": "dc",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "unexcused_absences",', fill=TRUE)
cat('\t"data": ',json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)


sink()
close(newfile)

## VALIDATE JSON
test <- readLines("unexcused_absences.json")
validate(test)