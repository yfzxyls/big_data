import math

#ax^2 + bx + c = 0
def quadratic(a, b, c):
    d = b*b-4*a*c
    if(d<0): return '无解'
    x1 = (-b+math.sqrt(b*b-4*a*c))/(2*a)
    x2 = (-b-math.sqrt(b*b-4*a*c))/(2*a)
    return(x1,x2)

print(quadratic(1,2,3))