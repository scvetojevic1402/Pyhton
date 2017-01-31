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
    

#Python code line by line profiling:
#sudo easy_install line_profiler
#for memory usage use:
#sudo pip install memory_profiler
#sudo pip install psutil
#line profiler:
#@profile
#def main():...
#Usage: kernprof -l -v script.py

#For searches always use sets!!!
#Much faster than bisect, numpy.searchsorted or a custom defined binary search function!!!
#A python dictionary is as fast as a set but takes longer to instantiate (let's say all the values are simply 1 d={x:1 for x in some_list}) and takes up more memory

#kernprof -l -v fast_set_search.py
#Function: main at line 19

#Line #      Hits         Time  Per Hit   % Time  Line Contents
#==============================================================
#    19                                           @profile
#    20                                           def main():
#    21         1       144922 144922.0      4.7      c=np.random.randint(low=0,high=10000000,size=10000000)
#    22         1       332271 332271.0     10.7      l=list(c)
#    23         1      1390709 1390709.0     44.8      s=set(l)
#    24         1            3      3.0      0.0      if 234234 in s:
#    25                                                   print 1
#    26         1       611344 611344.0     19.7      if 234234 in l:
#    27                                                   print 1
#    28         1           15     15.0      0.0      bisect.bisect(l,234234)
#    29         1       622612 622612.0     20.1      np.searchsorted(l,234234)
#    30         1           41     41.0      0.0      binary_search(l,234234)
