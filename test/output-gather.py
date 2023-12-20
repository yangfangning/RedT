import os  

def extractParam(filename, paramTar):
    fileObj = open(filename)
    lastLine = fileObj.readlines()[-1]
    tokens = lastLine.split(',')
    for param in tokens:
        pair = param.split('=')
        if pair[0] == paramTar:
            return pair[1]
    return "error"

def main():
    targetParam = "tput"
    fileType = ["MAAT_","MVCC_"]
    direPath = "./"
    nums = ["0.0", "0.25", "0.5", "0.55", "0.6", "0.65", "0.7", "0.75", "0.8", "0.9"]
    results = []
    for cc in fileType:
        for i in nums:
            filename = direPath + "output_" + cc + i + ".txt"
            paramValue = extractParam(filename, targetParam)
            results+=[((filename, paramValue))]
    for item in results:
            print("filename:", item[0],targetParam+":", item[1])

main()

# 这段代码的作用是从多个文件中提取一个指定参数的值，并将结果打印出来。代码的具体步骤如下：

# 1. 导入os模块
# 2. 定义一个函数extractParam，该函数接受两个参数：文件名和目标参数名。函数的作用是打开文件，读取文件的最后一行，将该行按逗号分隔成多个参数，然后遍历每个参数，将参数按等号分隔成键值对，如果键等于目标参数名，则返回该参数的值；否则返回"error"。
# 3. 定义一个函数main，该函数没有参数。函数的作用是定义目标参数名、文件类型、文件夹路径、参数值列表和结果列表。然后遍历文件类型和参数值列表，构造文件名，调用extractParam函数提取目标参数的值，并将该文件名和参数值组成一个元组，添加到结果列表中。
# 4. 遍历结果列表，打印每个元组的文件名和目标参数名及其对应的值。
# 5. 调用main函数。
