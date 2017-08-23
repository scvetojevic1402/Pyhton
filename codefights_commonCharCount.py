#Given two strings, find the number of common characters between them.

#Example

#For s1 = "aabcc" and s2 = "adcaa", the output should be
#commonCharacterCount(s1, s2) = 3.

#Strings have 3 common characters - 2 "a"s and 1 "c".


def commonCharacterCount(s1, s2):
    num=0
    s1_matches=[]
    s2_matches=[]    
    for i in range(0,len(s1)):
        if i not in s1_matches:
            for j in range(0,len(s2)):
                if j not in s2_matches:
                    if s1[i]==s2[j]:
                        num+=1
                        s1_matches.append(i)
                        s2_matches.append(j)
                        break
    return num
