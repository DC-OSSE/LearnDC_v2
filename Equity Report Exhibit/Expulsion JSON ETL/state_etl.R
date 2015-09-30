setwd("X:/Learn DC/State Equity Report Development")
source("U:/R/tomkit.R")
library(jsonlite)
library(reshape2)
library(dplyr)

exp <- sqlQuery(dbrepcard_prod, "SELECT [School_Code], [School_Year], [Student_Group], [Metric], [NSize], [SchoolScore], [AverageScore]
                FROM [dbo].[equity_longitudinal3] WHERE [Metric] in ('Expulsion Rate','Expulsions')")

exp_long <- melt(exp, id.vars = c("School_Year", "School_Code", "Student_Group", "Metric"))
exp_long$Metric <- paste0(exp_long$Metric, "_",exp_long$variable)
exp_long$Metric <- gsub(" ","_",exp_long$Metric)
exp_long$variable <- NULL

exp_wide <- dcast(exp_long, School_Year + School_Code + Student_Group ~ Metric, value.var = "value")
colnames(exp_wide) <- c("year","school_code","subgroup","state_expulsion_rate","explusion_rate_n","expulsion_rate","state_expulsions","expulsions_n","expulsions")
exp_wide$explusion_rate_n <- NULL
exp_wide$expulsions_n <- NULL
exp_wide$state_expulsion_rate <- exp_wide$state_expulsion_rate/100
exp_wide$expulsion_rate <- exp_wide$expulsion_rate/100
exp_wide <- select(exp_wide, school_code, year,subgroup, expulsions, expulsion_rate, state_expulsions, state_expulsion_rate)
exp_wide$school_code <- sapply(exp_wide$school_code, leadgr, 4)

##OPTION 2
state_vals <- exp_wide[which(exp_wide$school_code=='0161'),]
state_vals$expulsions <- state_vals$state_expulsions
state_vals$expulsion_rate <- state_vals$state_expulsion_rate

state_exp_all <- state_vals %>%
  mutate(population='All',state_expulsions=NA,state_expulsion_rate=NA) %>%
  select(year,subgroup,population,expulsions,expulsion_rate,state_expulsions,state_expulsion_rate)

state_exp_gen <- state_vals %>%
  mutate(population='Gen',expulsions=round(expulsions/1.25,0),expulsion_rate=round(expulsion_rate/1.25,2),
    state_expulsions=NA,state_expulsion_rate=NA) %>%
  select(year,subgroup,population,expulsions,expulsion_rate,state_expulsions,state_expulsion_rate)

state_exp_alt <- state_vals %>%
  mutate(population='Alt',expulsions=round(expulsions/1.75,0),expulsion_rate=round(expulsion_rate/1.75,2),
    state_expulsions=NA,state_expulsion_rate=NA) %>%
  select(year,subgroup,population,expulsions,expulsion_rate,state_expulsions,state_expulsion_rate)

state_exp <- rbind.data.frame(state_exp_all,state_exp_gen,state_exp_alt)

strtable(state_exp)

key_index <- 1:3
value_index <- 4:7

setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

nested_list <- lapply(1:nrow(state_exp), FUN = function(i){ 
  list(key = list(state_exp[i,key_index]), 
       val = list(state_exp[i,value_index]))
})

json <- toJSON(nested_list, na="null")
json <- gsub("[[","",json, fixed=TRUE)
json <- gsub("]]","",json, fixed=TRUE)
json <- prettify(json)


newfile <- file("expulsions.json", encoding="UTF-8")
sink(newfile)

cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "state",', sep="", fill=TRUE)
cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
cat('"org_code": "dc",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "expulsions",', fill=TRUE)
cat('\t"data": ',json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)


sink()
close(newfile)

## VALIDATE JSON
test <- readLines("expulsions.json")
validate(test)