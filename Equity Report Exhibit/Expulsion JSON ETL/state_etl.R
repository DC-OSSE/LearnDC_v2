source(paste(root_dir,'imports/helpers.R',sep=''))
library(jsonlite)
library(reshape2)
library(dplyr)


expel <- sqlQuery(dbrepcard_prod,"select * from equity_report_state_longitudinal where school_year = '2016-2017' and reported=1 and metric in ('Total Expulsions','Expulsion Rate')") %>%
mutate(subgroup=ifelse(subgroup %in% c('Male','Female'),toupper(subgroup),subgroup)) %>% select(-starts_with("days"),-school_year,-grade,-starts_with("mo"),-starts_with("re"),-count,-nsize,-enrollment)

exp_long <- melt(expel, id.vars = c("year","subgroup","metric","population"))
exp_long$metric <- paste0(exp_long$metric, "_",exp_long$variable)
exp_long$variable <- NULL
exp_wide <- dcast(exp_long,year + subgroup + population ~ metric,value.var="value")
colnames(exp_wide) <- c("year","subgroup","population","expulsion_rate","expulsions")
exp_wide$state_expulsions <- NA
exp_wide$state_expulsion_rate <- NA
exp_wide <- exp_wide[c(1:3,5,4,6:7)]

exp_wide$expulsion_rate <- round(exp_wide$expulsion_rate, 4)

strtable(exp_wide)

key_index <- 1:3
value_index <- 4:7

setwd(paste(root_dir, 'Export/JSON/state/DC', sep=''))

nested_list <- lapply(1:nrow(exp_wide), FUN = function(i){ 
  list(key = list(exp_wide[i,key_index]), 
       val = list(exp_wide[i,value_index]))
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