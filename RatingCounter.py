#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob

# Put all of our MapReduce information into an MRJob class
class MRRatingCounter(MRJob):
    # The Mapper is extracting and organizing the data that 
    # we care about (moving forward) into Key-Value pairs
    # Key: Ratings
    # Value: 1(occurence)
    
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1
    
    # Between the Mapper and Reducer, take list of key-value
    # pairs (from Mapper) and sort by keys and groups together
    # all values for that key
    
    # In the Reducer: for each key, the Reducer is called once, 
    # and it will sum the number of 1's (occurences) in the list
    
    def reducer(self, rating, occurences):
        yield rating, sum(occurences)
    
        
if __name__ == '__main__':
    MRRatingCounter.run()


# Save the code file as RatingCounter.py 
# in the same folder and execute the following command

#!python RatingCounter.py ml-100k/u.data