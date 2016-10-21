setwd(dir = "/Users/zuodina/Desktop/CME/Gmail")
data = read.csv(file = "spy organized.csv")
data.spy = data[c("exe.p.d",
                  "exe.v","exe.v.c",
                  "best.bid.q.1","best.ask.q.1",
                  "imbalance",
                  "best.bid.price.1.c","best.ask.price.1.c",
                  "best.bid.q.1.c","best.ask.q.1.c")]

colnames(data.spy) = c("exe.p.d",
                                       "execution volume","execution volume change",
                                       "best bid quantity","best ask quantity",
                                       "imbalance",
                                       "best bid price change","best ask price change",
                                       "best bid quantity change","best ask quantity change")
# 把y设置成分类变量。若不设置，则运行回归分析。
data.spy$exe.p.d = as.factor(data.spy$exe.p.d)
data.spy = data.spy[-1,]

y = data.spy$exe.p.d
x = data.spy[,c(2:10)]

y = y[-1]
x = x[-1,]
data.spy = cbind(y,x)

library(randomForest)
n.train = 8000
train = data.spy[c(1:n.train),]
test = data.spy[-c(1:n.train),]

# 包含价格不变的情况。
my.rf.1 = randomForest(train[,c(2:10)],train[,1],importance = T, mtry = 3, ntree = 1000)
table(predict(my.rf.1),train$y)
prediction = predict(my.rf.1,newdata = test)
table(prediction,test$y)

prediction.2 = predict(my.rf.1,newdata = data.spy)
qi.liu = cbind(prediction.2,data.spy)
write.csv(qi.liu,file = "data for liu qi.csv",row.names = F)

importance(my.rf.1,type = 1)
varImpPlot(my.rf.1,sort = T,main = "Evaluation of Input Importance")

