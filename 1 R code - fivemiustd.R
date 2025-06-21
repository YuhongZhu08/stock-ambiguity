library(dplyr)
library(xts)
library(zoo)
library(PerformanceAnalytics)  
library(data.table)  #不要lubridate
library("parallel")
library("iterators")
library("foreach")
library("doParallel")
library(snow)
library(doSNOW)

library(dplyr)
library(xts)
library(zoo)
library(PerformanceAnalytics)  
library(data.table)  #不要lubridate
library("parallel")
library("iterators")
library("foreach")
library("doParallel")
library(snow)
library(doSNOW)



year='20204'

setwd(paste("G:/Temp",year, sep = ""))   #自己设置原始数据位置
     
Sys.setenv(TZ='Asia/Shanghai')

file_list <-list.files(pattern = "*.csv", recursive = TRUE)
file_list_agu <-file_list[(substr(file_list,17,20)=="SH60" |substr(file_list,17,21)=="SZ300" 
           |substr(file_list,17,20)=="SZ00" |substr(file_list,17,24)=="SH000001" |substr(file_list,17,24)=="SH000906") ]   
#设置股票范围，是根据文件路径筛选的，“file_list,17,20”这里的数据可能需要自己调整
length(file_list_agu)

cl.cores=detectCores(logical = F)
cl<-makeCluster(cl.cores)
registerDoSNOW (cl)


forma<-file_list_agu
pb <- txtProgressBar(max = length(forma), style = 3)
progress <- function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)
ptm<-proc.time()
x2<-foreach (filename = forma, .combine="rbind",
             .packages = c("data.table","dplyr","PerformanceAnalytics"),.options.snow = opts) %dopar% {

                 
   sample <- read.csv(filename, header = TRUE, stringsAsFactors=FALSE)
   sample <- sample[, !(colnames(sample)  != "Price" & colnames(sample)  != "Time" )] 
   sample <- sample[(sample$Time != "Time"),]
   sample <- sample[(sample$Price !=0),]
   sample$Time2<-as.POSIXct(sample$Time)
   sample$Time22<-(hour(sample$Time)-9)*3600+(minute(sample$Time)-30)*60+second(sample$Time)-1
   sample$group<- sample$Time22%/%300+1
   sample <- subset(sample,(group >0 & group <25) | (group >42 & group <67) | (Time22==-1) | (Time22==12599))  
   sample[(sample$group == 0 ),"group"]=1
   sample[(sample$group == 42 ),"group"]=43
   
   if (nrow(sample)<2) {c(0,0,0,0,0)} else{
  sample$Price<- as.numeric(sample$Price)
  sample <-arrange(sample,Time2)
  sample$half<- sample$group%/%43
  sample$dif2<-  c(diff(sample$group),1)
  sample$dif3<-  c(1,diff(sample$half))
  sample <- subset(sample,(dif2>0) | (dif3>0)) 
  sample$ret <- c(NA, diff(log(sample$Price)))
  sample <- subset(sample,dif3<1) 
  test.ret <- sample$ret

  c(as.character(sample$Time[1]),substring(filename,17,24),mean(test.ret),StdDev(test.ret),length(test.ret))
}
             }

proc.time()-ptm
#write.table(x2, file = paste("E:/fivemindata",year, ".csv",sep = ""),  row.names = FALSE, sep=",")
close(pb)
stopCluster(cl)


colnames(x2) <- c("time", "exstock","fivemiu","fivestd","countobs")
x3<-x2
x3[which(x3[,'exstock']=="SH000001"),'exstock']<-"SH700001"
x4<-as.data.frame(x3)
x4 <- x4[which(x4$exstock !=0),]
x4<-mutate(x4,ym = year(time)*100+month(time))
x4<-mutate(x4,ymd = year(time)*10000+month(time)*100+mday(time))
x4<-mutate(x4,wday = wday(time))
x4<-x4[(x4$wday %in% 2:6),]
x4<-x4[,-which(names(x4)%in%c("wday"))]
write.table(x4, file = paste("E:/dailyfivemiustd",year, ".csv",sep = ""),  row.names = FALSE, sep=",")
x4[1:5,]


