setwd("U:/LearnDC ETL V2/HQT Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(jsonlite)
library(plyr)


school_hqt <- read.csv("hqt_class_counts_02042015.csv")
school_hqt <- rename(school_hqt,c('school_year'='year','elem_sec_classification'='school_category'))
school_hqt$year[which(school_hqt$year=='2013-2014')] <- 2014
school_hqt$poverty_quartile[which(school_hqt$poverty_quartile %in% 2:3)] <- "NEITHER"
school_hqt$poverty_quartile[which(school_hqt$poverty_quartile %in% 1)] <- "LOW"
school_hqt$poverty_quartile[which(school_hqt$poverty_quartile %in% 4)] <- "HIGH"
school_hqt$school_category[which(school_hqt$school_category %in% c('SEC','SEC, ADULT'))] <- 'SEC'

school_hqt$school_code <- sapply(school_hqt$school_code, leadgr, 4)
school_hqt$school_name <- toupper(school_hqt$school_name)
school_hqt$lea_code <- sapply(school_hqt$lea_code, leadgr, 4)
school_hqt$lea_name <- toupper(school_hqt$lea_name)

sqlSave(dbrepcard_prod, school_hqt, tablename = "hqt_classes_school_exhibit_w2014", append = FALSE, rownames=FALSE)