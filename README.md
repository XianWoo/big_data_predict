# big_data_predict
Based on Naive Beyes algorithm.



# 朴素贝叶斯算法预测，以航班延误预测为例
## 步骤一 数据预处理
excel表格或使用hive或pig将csv文件按如下格式进行处理
|第一列|第二列|……|第n-1列|第n列|
- - - -
|与预测值相关的数据1|与预测值相关的数据2|……|与预测值相关的数据n-1|预测值|

注意点：
- 标题行删去
- 自行分割测试集与训练集为两个csv文件train.csv与test.csv

## 步骤二 上传文件
hadoop fs -put train.csv .
hadoop fs -put test.csv .


## 步骤三 运行程序
将fly.scala文件在命令行运行即可
*注意：fly.scala文件针对数据集进行了代码修改；如果要预测评分等。使用naivebeyes.scala文件

## 结果输出
退出到终端，输入指令
hadoop fs -cat output/part-00000
*飞机延误例子中结果如图
![8b2fa81566785d7ca1f79c537a42235](https://user-images.githubusercontent.com/44899736/147672633-3001f94e-8f3f-459d-89a1-028dd9f18218.png)
0为未延误，1为延误。
