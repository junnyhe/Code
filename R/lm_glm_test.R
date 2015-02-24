ins<-read.table("/Users/junhe/Dropbox/Documents/R/code/TACO2013_project/Data/data_ins_redc2.csv",header=TRUE,sep=",", fileEncoding="latin1")
oos<-read.table("/Users/junhe/Dropbox/Documents/R/code/TACO2013_project/Data/data_oos_redc2.csv",header=TRUE,sep=",",fileEncoding="latin1")
ins=ins[-1]
oos=oos[-1]

#Test linear model#
model_1 <- lm(BO_confirmed ~
F_TXN_PR_CA_GR_AM_BY_CR_MX3M+
H_NUM_MTH_RTNCK_0TO6M+
H_UTIL_CO_CRMAX+
F_TXN_W10_DLR_AM_BY_CR_3_6+
F_TXN_HR_MER_AM_BY_CR_3_6+
F_TXN_Charoff_amt+
F_TXN_PY_NET_AM_BY_CR_3_6+
F_LAT_LMT_CSH_AM_I_L3M+
INQR_6_MO_CN_Inclusive+
F_TXN_WHL_DLR_AM_BY_CR_L6M+
F_TXN_HR_MER4_AM_BY_CR_3_6+
F_TXN_WHL_DLR_AM_OV_CR_L3M+
F_TXN_PR_CA_GR_CN_L1M+
F_TXN_Util_Bal_CR_L6M+
F_TXN_PY_NET_CN_L6M+
H_IND_RTNCK_AT_0m+
TRD_OPN_3_MO_CN+
F_TXN_PY_NET_AM_BY_CR_L1M+
F_TXN_PY_NET_AM_OV_75CR_L1M
,ins)

#Test glm linear#
model_2 <- glm(BO_confirmed ~
F_TXN_PR_CA_GR_AM_BY_CR_MX3M+
H_NUM_MTH_RTNCK_0TO6M+
H_UTIL_CO_CRMAX+
F_TXN_W10_DLR_AM_BY_CR_3_6+
F_TXN_HR_MER_AM_BY_CR_3_6+
F_TXN_Charoff_amt+
F_TXN_PY_NET_AM_BY_CR_3_6+
F_LAT_LMT_CSH_AM_I_L3M+
INQR_6_MO_CN_Inclusive+
F_TXN_WHL_DLR_AM_BY_CR_L6M+
F_TXN_HR_MER4_AM_BY_CR_3_6+
F_TXN_WHL_DLR_AM_OV_CR_L3M+
F_TXN_PR_CA_GR_CN_L1M+
F_TXN_Util_Bal_CR_L6M+
F_TXN_PY_NET_CN_L6M+
H_IND_RTNCK_AT_0m+
TRD_OPN_3_MO_CN+
F_TXN_PY_NET_AM_BY_CR_L1M+
F_TXN_PY_NET_AM_OV_75CR_L1M
,family=gaussian, data=ins)

#Test Logistic#
model_3 <- glm(BO_confirmed ~
F_TXN_PR_CA_GR_AM_BY_CR_MX3M+
H_NUM_MTH_RTNCK_0TO6M+
H_UTIL_CO_CRMAX+
F_TXN_W10_DLR_AM_BY_CR_3_6+
F_TXN_HR_MER_AM_BY_CR_3_6+
F_TXN_Charoff_amt+
F_TXN_PY_NET_AM_BY_CR_3_6+
F_LAT_LMT_CSH_AM_I_L3M+
INQR_6_MO_CN_Inclusive+
F_TXN_WHL_DLR_AM_BY_CR_L6M+
F_TXN_HR_MER4_AM_BY_CR_3_6+
F_TXN_WHL_DLR_AM_OV_CR_L3M+
F_TXN_PR_CA_GR_CN_L1M+
F_TXN_Util_Bal_CR_L6M+
F_TXN_PY_NET_CN_L6M+
H_IND_RTNCK_AT_0m+
TRD_OPN_3_MO_CN+
F_TXN_PY_NET_AM_BY_CR_L1M+
F_TXN_PY_NET_AM_OV_75CR_L1M
,family=binomial(link=logit), data=ins)

logit<-function(x){exp(x)/(1+exp(x))}
bo_pred_3 = logit(predict(model_3,oos))
hist(bo_pred_3)
plot(bo_pred_3,oos$BO_confirmed)
table(bo_pred_3>quantile(bo_pred,0.97),oos$BO_confirmed)


#Test SVM#
library(e1071)
x=cbind(
ins$F_TXN_PR_CA_GR_AM_BY_CR_MX3M,
ins$H_NUM_MTH_RTNCK_0TO6M,
ins$H_UTIL_CO_CRMAX,
ins$F_TXN_W10_DLR_AM_BY_CR_3_6,
ins$F_TXN_HR_MER_AM_BY_CR_3_6,
ins$F_TXN_Charoff_amt,
ins$F_TXN_PY_NET_AM_BY_CR_3_6,
ins$F_LAT_LMT_CSH_AM_I_L3M,
ins$INQR_6_MO_CN_Inclusive,
ins$F_TXN_WHL_DLR_AM_BY_CR_L6M,
ins$F_TXN_HR_MER4_AM_BY_CR_3_6,
ins$F_TXN_WHL_DLR_AM_OV_CR_L3M,
ins$F_TXN_PR_CA_GR_CN_L1M,
ins$F_TXN_Util_Bal_CR_L6M,
ins$F_TXN_PY_NET_CN_L6M,
ins$H_IND_RTNCK_AT_0m,
ins$TRD_OPN_3_MO_CN,
ins$F_TXN_PY_NET_AM_BY_CR_L1M,
ins$F_TXN_PY_NET_AM_OV_75CR_L1M
)
y=ins$BO_confirmed
model_4 = svm(x, y)

xprm=cbind(
oos$F_TXN_PR_CA_GR_AM_BY_CR_MX3M,
oos$H_NUM_MTH_RTNCK_0TO6M,
oos$H_UTIL_CO_CRMAX,
oos$F_TXN_W10_DLR_AM_BY_CR_3_6,
oos$F_TXN_HR_MER_AM_BY_CR_3_6,
oos$F_TXN_Charoff_amt,
oos$F_TXN_PY_NET_AM_BY_CR_3_6,
oos$F_LAT_LMT_CSH_AM_I_L3M,
oos$INQR_6_MO_CN_Inclusive,
oos$F_TXN_WHL_DLR_AM_BY_CR_L6M,
oos$F_TXN_HR_MER4_AM_BY_CR_3_6,
oos$F_TXN_WHL_DLR_AM_OV_CR_L3M,
oos$F_TXN_PR_CA_GR_CN_L1M,
oos$F_TXN_Util_Bal_CR_L6M,
oos$F_TXN_PY_NET_CN_L6M,
oos$H_IND_RTNCK_AT_0m,
oos$TRD_OPN_3_MO_CN,
oos$F_TXN_PY_NET_AM_BY_CR_L1M,
oos$F_TXN_PY_NET_AM_OV_75CR_L1M
)
yprm = oos$BO_confirmed
pred<-predict(model_4,xprm)
hist(pred)
table(pred>quantile(pred,0.97),yprm)



#Test Decision Tree#
library(tree)
model_5 <- tree(BO_confirmed ~
F_TXN_PR_CA_GR_AM_BY_CR_MX3M+
H_NUM_MTH_RTNCK_0TO6M+
H_UTIL_CO_CRMAX+
F_TXN_W10_DLR_AM_BY_CR_3_6+
F_TXN_HR_MER_AM_BY_CR_3_6+
F_TXN_Charoff_amt+
F_TXN_PY_NET_AM_BY_CR_3_6+
F_LAT_LMT_CSH_AM_I_L3M+
INQR_6_MO_CN_Inclusive+
F_TXN_WHL_DLR_AM_BY_CR_L6M+
F_TXN_HR_MER4_AM_BY_CR_3_6+
F_TXN_WHL_DLR_AM_OV_CR_L3M+
F_TXN_PR_CA_GR_CN_L1M+
F_TXN_Util_Bal_CR_L6M+
F_TXN_PY_NET_CN_L6M+
H_IND_RTNCK_AT_0m+
TRD_OPN_3_MO_CN+
F_TXN_PY_NET_AM_BY_CR_L1M+
F_TXN_PY_NET_AM_OV_75CR_L1M
, data=ins)
bo_pred_5 = predict(model_5,oos)
hist(bo_pred_5)
plot(bo_pred_5,oos$BO_confirmed)


#Test Random Forest/not enough memory to run#
library(randomForest)
model_6 <- randomForest(BO_confirmed ~
F_TXN_PR_CA_GR_AM_BY_CR_MX3M+
H_NUM_MTH_RTNCK_0TO6M+
H_UTIL_CO_CRMAX+
F_TXN_W10_DLR_AM_BY_CR_3_6+
F_TXN_HR_MER_AM_BY_CR_3_6+
F_TXN_Charoff_amt+
F_TXN_PY_NET_AM_BY_CR_3_6+
F_LAT_LMT_CSH_AM_I_L3M+
INQR_6_MO_CN_Inclusive+
F_TXN_WHL_DLR_AM_BY_CR_L6M+
F_TXN_HR_MER4_AM_BY_CR_3_6+
F_TXN_WHL_DLR_AM_OV_CR_L3M+
F_TXN_PR_CA_GR_CN_L1M+
F_TXN_Util_Bal_CR_L6M+
F_TXN_PY_NET_CN_L6M+
H_IND_RTNCK_AT_0m+
TRD_OPN_3_MO_CN+
F_TXN_PY_NET_AM_BY_CR_L1M+
F_TXN_PY_NET_AM_OV_75CR_L1M
, ntree=100,data=ins)
bo_pred_6 = predict(model_6,oos)
plot(bo_pred_6,oos$BO_confirmed)
table(bo_pred_6>quantile(bo_pred_6,0.97),oos$BO_confirmed)



#Test Neural Net#
library(nnet)
x_ins=ins[-20]
y_ins=ins$BO_confirmed
x_oos=oos[-20]
y_oos=oos$BO_confirmed

index1=which(ins$BO_confirmed==1)
x_ins1=x_ins[index1,1:19] # use to add more weight
y_ins1=y_ins[index1]

x_ins_rw = rbind(x_ins)# can add x_ins1 in the ()
y_ins_rw = as.array(c( y_ins))# can add y_ins1 in the ()
y_ins_rw = cbind(y_ins_rw,1-y_ins_rw)

model_7 <- nnet(x=x_ins_rw,y=y_ins_rw, size = 6, rang=1,decay = 5e-6, maxit = 200)
aa=model_7$fitted.values
aa=aa[1:length(aa)/2,1]
mean(aa)
model_7$wts
bo_pred_7 = predict(model_7,x_oos, type = c("raw","class"))
dim_bp = dim(bo_pred_7)
bo_pred_7=1-bo_pred_7[,2]
hist(bo_pred_7)
table(bo_pred_7,y_oos)
plot(bo_pred_7,y_oos)



############## plot results #################
#logistic
plot(bo_pred_3,oos$BO_confirmed)

#tree
plot(bo_pred_5,oos$BO_confirmed)

#randomForest
plot(bo_pred_6,oos$BO_confirmed)

#nnet
plot(bo_pred_7,oos$BO_confirmed)


############## plot ROC and calculate AUC #################
library(pROC)






