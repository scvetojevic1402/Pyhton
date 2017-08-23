#Given a sequence of integers as an array, determine whether it is possible to obtain a strictly increasing sequence 
#by removing no more than one element from the array.

#Example

#For sequence = [1, 3, 2, 1], the output should be
#almostIncreasingSequence(sequence) = false;

#There is no one element in this array that can be removed in order to get a strictly increasing sequence.

#For sequence = [1, 3, 2], the output should be
#almostIncreasingSequence(sequence) = true.

#You can remove 3 from the array to get the strictly increasing sequence [1, 2]. 
#Alternately, you can remove 2 to get the strictly increasing sequence [1, 3].

def mergeSort(alist):
    if len(alist)>1:
        mid = len(alist)//2
        lefthalf = alist[:mid]
        righthalf = alist[mid:]

        mergeSort(lefthalf)
        mergeSort(righthalf)

        i=0
        j=0
        k=0
        while i < len(lefthalf) and j < len(righthalf):
            if lefthalf[i] < righthalf[j]:
                alist[k]=lefthalf[i]
                i=i+1
            else:
                alist[k]=righthalf[j]
                j=j+1
            k=k+1

        while i < len(lefthalf):
            alist[k]=lefthalf[i]
            i=i+1
            k=k+1

        while j < len(righthalf):
            alist[k]=righthalf[j]
            j=j+1
            k=k+1

def almostIncreasingSequence(sequence):
    ind=0
    for a in sequence:
        if a<-100000 or a>100000:
            break
    n=0
    for i in range(0,len(sequence)):
        s=list(sequence)
        s.pop(i)
        p = list(s)
        mergeSort(s)
        if p==s and len(p)==len(set(p)):
            print sequence[i],p
            return True
    return False
