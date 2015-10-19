require(RODBC)

dbrepcard <- odbcDriverConnect('driver={SQL Server};server=OSSEEDM1;database=reportcard_dev;trusted_connection=true')

dbworking <- odbcDriverConnect('driver={SQL Server};server=OSSEEDM1;database=working;trusted_connection=true')



