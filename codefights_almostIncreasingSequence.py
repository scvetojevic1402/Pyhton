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

def almostIncreasingSequence(sequence):
    for a in sequence:
        if a<-100000 or a>100000:
            return False
    for i in range(0,len(sequence)):
        if i == 0 or i==(len(sequence)-1):
            num = sequence.pop(i)
            #print i, num
            if sequence==sorted(sequence) and len(sequence)==len(set(sequence)): 
                return True
            sequence.insert(i,num)
        elif sequence[i]<=sequence[i-1] or sequence[i]>=sequence[i+1]:
            num = sequence.pop(i)
            #print num
            if sequence==sorted(sequence) and len(sequence)==len(set(sequence)): 
                return True
            sequence.insert(i,num)
        
    return False
