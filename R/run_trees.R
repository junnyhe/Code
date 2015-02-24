#load library#
library(rpart)
library(rpart.plot)

#load data#
ins<-read.table("/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/model_data_ds_ins_imp_woe.csv",header=TRUE,sep="|", fileEncoding="latin1")
#oos<-read.table("/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/model_data_ds_oos_imp_woe.csv",header=TRUE,sep="|",fileEncoding="latin1")

var_list=read.table("/Users/junhe/Documents/Results/R/tree_results/var_list_tree.csv",header=FALSE)
var_list=var_list$V1


for (var in var_list[149:length(var_list)] ){
	print( paste("training trees for",var) )
	
	model<-rpart(as.formula(paste("target~",'get(var)' )),data=ins, method="anova", maxdepth=3,cp = 0.001)
	#p_pred = predict(model,oos)
	#rpart.plot(model,type=4)
	if (nrow(model$frame)>1) {
		post(model, file = paste("/Users/junhe/Documents/Results/R/tree_results/tree_",var,"_reg.ps",sep=""), title = " ")
	}
	capture.output(model, file = paste("/Users/junhe/Documents/Results/R/tree_results/tree_",var,"_reg.txt",sep=""))

	
	model<-rpart(as.formula(paste("target~",'get(var)' )), data=ins, method="class",maxdepth=3,cp = 0.001)
	#p_pred = predict(model,oos)
	#rpart.plot(model,type=4)
	if (nrow(model$frame)>1) {
		post(model, file = paste("/Users/junhe/Documents/Results/R/tree_results/tree_",var,"_class.ps",sep=""), title = " ")
	}
	capture.output(model, file = paste("/Users/junhe/Documents/Results/R/tree_results/tree_",var,"_class.txt",sep=""))
	
}



#model<-rpart(target~signal_1,data=oos, weights= oos$target*10 +1,maxdepth=4,cp = 0.001)
