#Some people are standing in a row in a park. There are trees between them which cannot be moved. 
#Your task is to rearrange the people by their heights in a non-descending order without moving the trees.

#Example

#For a = [-1, 150, 190, 170, -1, -1, 160, 180], the output should be
#sortByHeight(a) = [-1, 150, 160, 170, -1, -1, 180, 190].



def sortByHeight(a):
    trees_ind=[]
    sorted_a=[]
    for i in range(0,len(a)):
        if a[i]==-1:
            trees_ind.append(i)
    print trees_ind
    if len(trees_ind)==0:
        return sorted(a)
    for b in a:
        if b!=-1:
            sorted_a.append(b)
    sorted_a=sorted(sorted_a)
    for i in trees_ind:
        sorted_a.insert(i,-1)
    
    return sorted_a
