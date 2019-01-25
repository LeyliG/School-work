import numpy as np

def LeyliSort(minimum, maximum, size):
    A = np.random.randint(minimum, maximum, size)
    count = 0
    for count in range(len(A)):
        temp = 0
        minimum = A[count]
        index = count 

        for i in range(count, len(A) - 1):       
            if minimum > A[i + 1]:
                minimum = A[i + 1]
                index = i + 1 

        temp = A[count]
        A[count] = A[index]
        A[index] = temp

    return(A)


# test the algorithm code
LeyliSort(3, 100, 50)