
subproc <- function(x, subgroup){
	if(subgroup == "All"){
		return(x)
	} else if(subgroup %in% c("LOW","HIGH","NEITHER")){
		return(subset(x, poverty_quartile == subgroup))
	} else if(subgroup %in% c("ELEM","SEC")){
		return(subset(x, school_category == subgroup))
	} 
}

