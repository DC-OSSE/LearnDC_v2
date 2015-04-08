setwd("U:/LearnDC ETL V2/HQT Classes Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(dplyr)

school_dir <- sqlQuery(dbrepcard,"select * from dbo.schooldir_sy1314")
school_dir <- select(school_dir,year=school_year,lea_code,lea_name,school_code,school_name,charter_status)
school_dir$year <- 2014
school_dir$school_code <- sapply(school_dir$school_code, leadgr, 3)
school_dir$lea_code <- sapply(school_dir$lea_code, leadgr, 3)

lookup_ent <- sqlQuery(dbedm,"select * from dbo.lookup_entities where school_year='2013-2014'")
lookup_ent <- select(lookup_ent,year=school_year,lea_code,school_code,elem_sec_classification,poverty_quartile_elem,poverty_quartile_sec)
lookup_ent$year <- 2014
lookup_ent$school_code <- sapply(lookup_ent$school_code, leadgr, 3)
lookup_ent$lea_code <- sapply(lookup_ent$lea_code, leadgr, 3)

school_dir <- merge(school_dir,lookup_ent,by=c('year','lea_code','school_code'),all.x=TRUE)

tmp2 <- data.frame()
for(i in unique(school_dir$school_code)){
     tmp <- subset(school_dir, school_code==i)
if(!is.na(tmp$poverty_quartile_elem)){
      	tmp$poverty_quartile <- tmp$poverty_quartile_elem[1]
	} else{
 	 	tmp$poverty_quartile <- tmp$poverty_quartile_sec[1]
	}
	tmp2 <- rbind(tmp2,tmp)
}
tmp2 <- select(tmp2,year,lea_code,lea_name,school_code,school_name,charter_status,school_category=elem_sec_classification,poverty_quartile)

school_hqt <- read.csv("hqt_class_counts_1314.csv")
school_hqt <- select(school_hqt,year=school_year,lea_code,school_code,num_total_classes,num_hq_classes,num_nhq_classes)
school_hqt$year[which(school_hqt$year=='2013-2014')] <- 2014
school_hqt$school_code <- sapply(school_hqt$school_code, leadgr, 3)
school_hqt$lea_code <- sapply(school_hqt$lea_code, leadgr, 3)


school_hqt_all <- merge(tmp2,school_hqt,by=c("year","lea_code","school_code"),all.x=TRUE)
school_hqt_all$school_name <- toupper(school_hqt_all$school_name)
school_hqt_all$lea_name <- toupper(school_hqt_all$lea_name)

# school_hqt_non <- subset(school_hqt_all,school_name %notin% c("APPLETREE EARLY LEARNING CENTER PCS COLUMBIA HEIGHTS","APPLETREE EARLY LEARNING CENTER PCS LINCOLN PARK","APPLETREE EARLY LEARNING CENTER PCS OKLAHOMA","APPLETREE EARLY LEARNING CENTER PCS SOUTHWEST (AMIDON)", "APPLETREE EARLY LEARNING PCS SOUTHEAST (DOUGLAS KNOLL)","BRIYA PUBLIC CHARTER SCHOOL","CARLOS ROSARIO INTERNATIONAL PCS HARVARD STREET CAMPUS","COMMUNITY COLLEGE PREPARATORY ACADEMY PCS","KIPP DC CONNECT ACADEMY","LATIN AMERICAN YOUTH CENTER CAREER ACADEMY","MAYA ANGELOU PCS-YOUNG ADULT LEARNING CENTER","ROOTS PCS","ST. COLETTA SPECIAL EDUCATION PCS","THE NEXT STEP/EL PRÓXIMO PASO PCS","YOUTHBUILD PCS","EARLY S.T.A.G.E.S. AT WALKER JONES EC","PRIVATE/RELIGIOUS SCHOOL ENROLLMENT (PRO)","RESIDENTIAL SCHOOLS","TUITION GRANT DCPS NON PUBLIC","LASHAWN - DCPS NON PUBLIC","CHILD & FAMILY SERVICES- REGULAR ED") & is.na(school_hqt_all$school_category))

# school_hqt_all <- subset(school_hqt_all, !is.na(school_hqt_all$school_category))

# school_hqt_non <- select(school_hqt_non,year,lea_code,lea_name,school_name,school_code,num_total_classes,num_hq_classes,num_nhq_classes)
# school_hqt_non <- merge(school_hqt_non,lookup_ent,by=c('year','lea_code','school_code'),all.x=TRUE)

# tmp2 <- data.frame()
# for(i in unique(school_hqt_non$school_code)){
#      tmp <- subset(school_hqt_non, school_code == i)
# if(!is.na(tmp$poverty_quartile_elem)){
#       	tmp$poverty_quartile <- tmp$poverty_quartile_elem[1]
# 	} else{
#  	 	tmp$poverty_quartile <- tmp$poverty_quartile_sec[1]
# 	}
# 	tmp2 <- rbind(tmp2,tmp)
# }

# school_hqt_non <- select(tmp2,year,lea_code,lea_name,school_name,school_code,school_category=elem_sec_classification,poverty_quartile,num_total_classes,num_hq_classes,num_nhq_classes)
# school_hqt_non$poverty_quartile[which(school_hqt_non$poverty_quartile %in% 2:3)] <- "MIDDLE"
# school_hqt_non$poverty_quartile[which(school_hqt_non$poverty_quartile %in% 1)] <- "LOW"
# school_hqt_non$poverty_quartile[which(school_hqt_non$poverty_quartile %in% 4)] <- "HIGH"

# school_hqt_all <- rbind(school_hqt_all,school_hqt_non)
# school_hqt_all <- select(school_hqt_all,year,lea_code,lea_name,school_name,school_code,school_category,poverty_quartile,num_total_classes,num_hq_classes,num_nhq_classes)

school_hqt_all$poverty_quartile[which(school_hqt_all$poverty_quartile %in% 2:3)] <- "MIDDLE"
school_hqt_all$poverty_quartile[which(school_hqt_all$poverty_quartile %in% 1)] <- "LOW"
school_hqt_all$poverty_quartile[which(school_hqt_all$poverty_quartile %in% 4)] <- "HIGH"
school_hqt_all <- subset(school_hqt_all,school_category %in% c('ELEM','SEC',"SEC, ADULT"))
school_hqt_all$school_category[which(school_hqt_all$school_category %in% c('SEC, ADULT'))] <- 'SEC'
school_hqt_all$num_total_classes[which(is.na(school_hqt_all$num_total_classes))] <- 0
school_hqt_all$num_hq_classes[which(is.na(school_hqt_all$num_hq_classes))] <- 0
school_hqt_all$num_nhq_classes[which(is.na(school_hqt_all$num_nhq_classes))] <- 0
school_hqt_all$charter_status[which(school_hqt_all$charter_status=='Yes')] <- "PCS"
school_hqt_all$charter_status[which(school_hqt_all$charter_status=='No')] <- "DCPS"

school_hqt_all <- subset(school_hqt_all,school_code %notin% c("0173","1047","0168","6000"))
school_hqt_all <- select(school_hqt_all,year,lea_code,lea_name,school_code,school_name,school_category,poverty_quartile,num_total_classes,num_hq_classes,num_nhq_classes)

sqlSave(dbrepcard_prod,school_hqt_all, tablename = "hqt_classes_school_exhibit", append = FALSE, rownames=FALSE)