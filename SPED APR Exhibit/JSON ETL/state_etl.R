source(paste(root_dir,'imports/helpers.R',sep=''))

library(jsonlite)
library(plyr)

lea_sped <- sqlQuery(dbrepcard_prod, "SELECT * FROM [dbo].[apr] WHERE lea = 'state'")

lea_sped$lea_code <- sapply(lea_sped$lea_code, leadgr, 4)
lea_sped$on_target <- ifelse(is.na(lea_sped$val), NA, lea_sped$on_target %in% c('1'))

key_index <- c(1,2)
value_index <- 5:10

setwd(paste(root_dir, 'Export/JSON/state/DC', sep=''))

.nested_list <- lapply(1:nrow(lea_sped), FUN = function(i){ 
                         list(key = list(lea_sped[i,key_index]), 
                         	val = list(lea_sped[i,value_index]))
                       })

.json <- toJSON(.nested_list, na="null")
.json <- gsub("[[","",.json, fixed=TRUE)
.json <- gsub("]]","",.json, fixed=TRUE)
.json <- prettify(.json)

newfile <- file("sped_apr.json", encoding="UTF-8")
sink(newfile)

cat('{', fill=TRUE)

cat('"timestamp": "',date(),'",', sep="", fill=TRUE)
cat('"org_type": "state",', sep="", fill=TRUE)
cat('"org_name": "District of Columbia",', sep="", fill=TRUE)
cat('"org_code": "dc",', sep="", fill=TRUE)
cat('"exhibit": {', fill=TRUE)
cat('\t"id": "sped_apr",', fill=TRUE)
cat('\t"data": ',.json, fill=TRUE)
cat('\t}', fill=TRUE)
cat('}', fill=TRUE)

sink()
close(newfile)