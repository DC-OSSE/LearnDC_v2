
gradeproc <- function(x, subgroup){
	if(subgroup == "All"){
		return(x)
	} else if(subgroup %in% c("PK3","PK4","KG","01","02","03","04","05","06","07","08","09","10","11","12","13","AO","UN")){
		return(subset(x, subgroup == grade))
	}
}