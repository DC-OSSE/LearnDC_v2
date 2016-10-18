source(paste(root_dir,'imports/helpers.R',sep=''))
library(dplyr)
library(jsonlite)

acc <- sqlQuery(dbrepcard,"select * from dbo.accountability_sy1617")
acc_sg <- sqlQuery(dbrepcard,"select * from dbo.accountability_sy1314_sg")

acc$school_code <- sapply(acc$school_code, leadgr, 4)
acc$lea_code <- sapply(acc$lea_code, leadgr, 4)
acc$year <- '2013'
acc_sg$school_code <- sapply(acc_sg$school_code, leadgr, 4)
acc_sg$lea_code <- sapply(acc_sg$lea_code, leadgr, 4)

for(i in unique(acc$school_code)){
  setwd(paste(root_dir, 'Export/JSON/school', sep=''))
  
  if(file.exists(i)){
    setwd(file.path(i))
  } else {
    num_orphans <- num_orphans + 1
  }
  
  .tmp <- subset(acc, school_code == i)
  .school_name <- .tmp$school_name[1]
  
  ## write to file
  newfile <- file("accountability.json", encoding="UTF-8")
  
  sink(newfile)
  cat('{', fill=TRUE)
  
  cat('\t"timestamp": "',date(),'",', sep="", fill=TRUE)
  cat('\t"org_type": "school",', sep="", fill=TRUE)
  cat('\t"org_name": "',gsub("\n", "",.school_name),'",', sep="", fill=TRUE)
  cat('\t"org_code": "',i,'",', sep="", fill=TRUE)
  cat('\t"exhibit": {', fill=TRUE)
  cat('\t\t"id": "accountability",', fill=TRUE)
  cat('\t\t"data": [{', fill=TRUE)
  cat('\t\t\t\t"year": ',checkna_str(.tmp$year[1]),',', sep="", fill=TRUE)
  cat('\t\t\t\t"score": ',checkna(.tmp$acct_score[1]),',', sep="", fill=TRUE)
  cat('\t\t\t\t"classification": ',checkna_str(.tmp$classification[1]),',', sep="", fill=TRUE)
  cat('\t\t\t\t"growth": ',checkna(.tmp$growth[1]),',', sep="", fill=TRUE)
  cat('\t\t\t\t"subgroups": [', fill=TRUE)
  .tmp_sg <- subset(acc_sg, lea_code == .tmp$lea_code[1] & school_code == i & (read_size >= 25 | math_size >= 25 | comp_size >= 25))
  if(nrow(.tmp_sg) > 0){
    for(j in 1:nrow(.tmp_sg)){
      cat('\t\t\t\t\t{', fill=TRUE)
      cat('\t\t\t\t\t\t"subgroup": ',checkna_str(.tmp_sg$subgroup[j]),',', sep="", fill=TRUE)
      cat('\t\t\t\t\t\t"read_size": ',checkna(.tmp_sg$read_size[j]),',', sep="", fill=TRUE)
      cat('\t\t\t\t\t\t"read_score": ',checkna(.tmp_sg$read_score[j]),',', sep="", fill=TRUE)
      cat('\t\t\t\t\t\t"math_size": ',checkna(.tmp_sg$math_size[j]),',', sep="", fill=TRUE)
      cat('\t\t\t\t\t\t"math_score": ',checkna(.tmp_sg$math_score[j]),',', sep="", fill=TRUE)
      cat('\t\t\t\t\t\t"comp_size": ',checkna(.tmp_sg$comp_size[j]),',', sep="", fill=TRUE)
      cat('\t\t\t\t\t\t"comp_score": ',checkna(.tmp_sg$comp_score[j]),'', sep="", fill=TRUE)
      if(j == nrow(.tmp_sg)){
        cat('\t\t\t\t\t}', fill=TRUE)
      } else {
        cat('\t\t\t\t\t},', fill=TRUE)
      }
    }
  }
  cat('\t\t\t\t]', fill=TRUE)
  cat('\t\t\t}', fill=TRUE)
  cat('\t\t]', fill=TRUE)
  cat('\t}', fill=TRUE)
  cat('}', fill=TRUE)
  
  sink()
  close(newfile)
}