#Given an array of strings, return another array containing all of its longest strings.

#Example

#For inputArray = ["aba", "aa", "ad", "vcd", "aba"], the output should be
#allLongestStrings(inputArray) = ["aba", "vcd", "aba"].


def allLongestStrings(inputArray):
    max_length=0
    longest_strings=[]
    for a in inputArray:
        if len(a)>max_length:
            max_length=len(a)
    print max_length
    for a in inputArray:
        if len(a)==max_length:
            longest_strings.append(a)
    return longest_strings
