#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob

class MRFriendsByAge(MRJob):

    # Key: Age
    # Value: Number of Friends
    
    def mapper(self, _, line):
        (ID, name, age, numFriends) = line.split(',')
        yield age, float(numFriends)

    def reducer(self, age, numFriends):
        total = 0
        numElements = 0
        for x in numFriends:
            total += x
            numElements += 1
            
        yield age, total / numElements

if __name__ == '__main__':
    MRFriendsByAge.run()


# In[ ]:


# Save the code file as RatingCounter.py 
# in the same folder and execute the following command

#!python RatingCounter.py ml-100k/u.data

