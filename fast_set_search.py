import bisect
import numpy as np
#from memory_profiler import profile

def binary_search(list, x):
    min_ind = 0
    max_ind = len(list) - 1
    while True:
        if max_ind < min_ind:
            return -1
        middle = (min_ind + max_ind) // 2
        if list[middle] < x:
            min_ind = middle + 1
        elif list[middle] > x:
            max_ind = middle - 1
        else:
            return middle

@profile
def main():
    c=np.random.randint(low=0,high=10000000,size=10000000)
    l=list(c)
    s=set(l)
    if 234234 in s:
        print 1
    if 234234 in l:
        print 1
    bisect.bisect(l,234234)
    np.searchsorted(l,234234)
    binary_search(l,234234)

if __name__=="__main__": main()
