#After becoming famous, CodeBots decided to move to a new building and live together. 
#The building is represented by a rectangular matrix of rooms, each cell containing an integer - the price of the room. 
#Some rooms are free (their cost is 0), but that's probably because they are haunted, so all the bots are afraid of them. 
#That is why any room that is free or is located anywhere below a free room in the same column is not considered suitable 
#for the bots.

#Help the bots calculate the total price of all the rooms that are suitable for them.

#Example

#For

#matrix = [[0, 1, 1, 2], 
#          [0, 5, 0, 0], 
#          [2, 0, 3, 3]]
#the output should be
#matrixElementsSum(matrix) = 9.

def matrixElementsSum(matrix):
    sum = 0
    zero_ind=[]
    matrix_width = len(matrix[0])
    for i in range(0,len(matrix[0])):
        sum+=matrix[0][i]
        if matrix[0][i]==0:
            zero_ind.append(i)
    
    for i in range(1,len(matrix)):
        for j in range(0,len(matrix[i])):
            if j not in zero_ind:
                sum+=matrix[i][j]
                if matrix[i][j]==0:
                    zero_ind.append(j)         
    return sum
