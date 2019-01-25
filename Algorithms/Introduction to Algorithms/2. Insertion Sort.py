import numpy as np

def LeyliInsert(minimum, maximum, size):
    #A = np.random.randint(minimum, maximum, size)
    A = [5, 4, 3, 2, 1]
    print(A)
    
    for a in range(1, len(A)):
        key = A[a]
        b = a
            
        while (b > 0) & (key < A[b - 1]):
            A[b] = A[b - 1]
            b = b - 1
            
        A[b] = key
            
    return(A)

# test the algorithm code
LeyliInsert(0, 5, 5)