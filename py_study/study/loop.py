# input 返回值为字符串
# if 块后以 冒号 ：
# birth = input('birth: ')
# birth = int(birth)
# if birth < 1990 :
#     print('90后')
# elif birth < 2000 :
#     print('00后')
# else :
# 	print('无后')

# for 循环
names = ['Michael', 'Bob', 'Tracy']
for name in names:
    print(name)	

# range 从0 开始 到 101 不包括101
sum = 0
for i in range(101):
	sum += i
print(sum)	  

#while 循环
i = 100
sum1 = 0
while  i > 0:
	sum1 += i
	if(i == 50) : break
	i = i-1
print(sum1)	

#dict
d = {'Michael': 95, 'Bob': 75, 'Tracy': 85}
print(d)
print(d['Michael'])
#判断 key 是否存在
print('Thoms' in d)
print('Michael' in d)
#获取key 
print(d.get('Michael'))
#key 不存在返回None
#注意：返回None的时候Python的交互环境不显示结果。
print(d.get('Thomas'))
#获取key  没有返回默认值
print(d.get('Thomas', -1))
#删除key 以及对应的value 并返回value 
print(d.pop('Michael'))
print(d)

d1 = {'B': 95, 'a': 75, 'b': 85}
print(d1)
for k in d1
	print('a'+k)
