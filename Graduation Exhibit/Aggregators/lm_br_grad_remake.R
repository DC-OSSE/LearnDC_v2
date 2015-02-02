##Most Recent Graduation Data Jumble
source("U:/R/tomkit.R")
##a -> 2009 cohort 4 and 5 year ACGR data
a <- sqlQuery(dbworking,"select * from working.dbo.grad0910_6")

##b -> 2007,2008 graduation data from the reportcard_dev.dbo.graduation table (eventually will be deprecated)
b <- sqlQuery(dbworking,"select * from working.dbo.grad0910_3")

##c -> 2010 4 year ACGR data
c <- sqlQuery(dbworking,"select * from working.dbo.grad0910_5")

##Perform union and apply minor changes
d <- rbind(a,b,c)
d$school_code <- sapply(d$school_code, leadgr, 4)
d$school_name <- toupper(d$school_name)
d$lea_code <- sapply(d$lea_code, leadgr, 4)
d$lea_name <- toupper(d$lea_name)

sqlSave(dbrepcard, d, tablename = "graduation_w2014b", append = FALSE, rownames=FALSE)