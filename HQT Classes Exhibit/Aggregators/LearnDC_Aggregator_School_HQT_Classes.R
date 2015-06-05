setwd("U:/LearnDC ETL V2/HQT Classes Exhibit/Aggregators")
source("U:/R/tomkit.R")
library(dplyr)

school_hqt <- sqlFetch(dbrepcard,"dbo.hq_status_wrevisions")
school_hqt$school_code <- sapply(school_hqt$school_code, leadgr, 3)
school_hqt$lea_code <- sapply(school_hqt$lea_code, leadgr, 3)
school_hqt$year <- ifelse(school_hqt$school_year=='2012-2013',2013,2014)
school_hqt <- subset(school_hqt,core_ind %in% c('y','Y'))
school_hqt <- subset(school_hqt,teacher_of_record %in% c('Y'))
school_hqt <- subset(school_hqt,year==2014)
school_hqt <- subset(school_hqt,staff_role %notin% c('Teacher, Pre-Kindergarten'))
school_hqt$staff_role[which(school_hqt$staff_role=='Teacher, Kindergarten')] <- 'Teacher, Elementary'
school_hqt$elem_sec_classification <- ifelse(school_hqt$staff_role=='Teacher, Elementary', 'ELEM','SEC')
school_hqt <- subset(school_hqt,school_code %notin% c('none','432','342'))

hqt_classification <- sqlFetch(dbrepcard,"dbo.faculty_staff_school_classifications_sy1314")
hqt_classification$school_code <- sapply(hqt_classification$school_code, leadgr, 3)
hqt_classification$lea_code <- sapply(hqt_classification$lea_code, leadgr, 3)
hqt_classification$year <- ifelse(hqt_classification$school_year=='2013-2014',2014,NA)
hqt_pq <- unique(hqt_classification[,c('year','lea_code','school_code','poverty_quartile')])
school_hqt <- merge(school_hqt,hqt_pq,by=c('year','lea_code','school_code'),all.x=TRUE)
school_hqt$poverty_quartile <- ifelse(school_hqt$poverty_quartile %in% 2:3,"MIDDLE",ifelse(school_hqt$poverty_quartile==1,"LOW","HIGH"))

hqt_school <- data.frame()
for(a in unique(school_hqt$school_code)){
    code_hqt <- subset(school_hqt,school_code==a)
    school_code <- a
    school_name <- code_hqt$school_name[1]
    lea_code <- code_hqt$lea_code[1]
    lea_name <- code_hqt$lea_name[1]
    
    for(b in unique(code_hqt$year)){
    	year_hqt <- subset(code_hqt,year==b)
    	year <- year_hqt$year[1]

    		for(c in unique(year_hqt$elem_sec_classification)){
    			class_hqt <- subset(year_hqt,elem_sec_classification==c)
    			school_category <- c
    			poverty_quartile <- class_hqt$poverty_quartile[1]
 	
    			num_hqt <- nrow(class_hqt[which(class_hqt$hq_decision=='Y'),])
    			num_non_hqt <- nrow(class_hqt[which(year_hqt$hq_decision!='Y'),])
    			num_total <- nrow(class_hqt)
    
    		new_row <- c(year,lea_code,lea_name,school_code,school_name,school_category,poverty_quartile,num_total,num_hqt,num_non_hqt)
    		hqt_school <- rbind.data.frame(hqt_school,new_row)
		}
	}
}
colnames(hqt_school) <- c("year","lea_code","lea_name","school_code","school_name","school_category","poverty_quartile","num_total_classes","num_hq_classes","num_nhq_classes")


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
tmp2 <- select(tmp2,year,lea_code,lea_name,school_code,school_name,school_category=elem_sec_classification,poverty_quartile)
non_report <- subset(tmp2,tmp2$school_code %notin% hqt_school$school_code)
non_report <- subset(non_report,lea_code %notin% c('103','162','119','176','143','145','131','133') & school_code %notin% c('958','6000','948','265','312','7000','472','465','209'))
non_report$school_name <- toupper(non_report$school_name)
non_report$lea_name <- toupper(non_report$lea_name)
non_report$poverty_quartile <- ifelse(non_report$poverty_quartile %in% 2:3,"MIDDLE",ifelse(non_report$poverty_quartile==1,"LOW","HIGH"))
non_report$num_total_classes <- 0
non_report$num_hq_classes <- 0
non_report$num_nhq_classes <- 0

hqt_school <- rbind.data.frame(hqt_school,non_report)

sqlSave(dbrepcard_prod,hqt_school, tablename = "hqt_classes_school_exhibit_bk", append = FALSE, rownames=FALSE)