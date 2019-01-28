from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    # need to define steps
    # two mappers and two reducers as following
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_make_counts_key,
                   reducer = self.reducer_output_words)
        ]

    # Key: Occurrence Value
    # Value: word
    
    def mapper_get_words(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer_count_words(self, word, values):
        yield word, sum(values)

    def mapper_make_counts_key(self, word, count):
        # adds nine 0's before the actual count 
        yield '%09d'%int(count), word

        # reducer outputs by count
    def reducer_output_words(self, count, words):
        for word in words:
            yield count, word

if __name__ == '__main__':
    MRWordFrequencyCount.run()

# WordFrequencySorted.py
# !python WordFrequencySorted.py pg100.txt > wordcountsorted.txt