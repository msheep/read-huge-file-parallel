# coding: utf-8
import sys, re, math
from collections import Counter
from mpi4py import MPI

if len(sys.argv) >= 2 and sys.argv[1]:
    term = sys.argv[1]
else:
    term = "Melbourne"

time_start = MPI.Wtime()

comm = MPI.COMM_WORLD  
comm_rank = comm.Get_rank()  
comm_size = comm.Get_size()  
chunksize = 1024*1024*512

if comm_rank == 0:
    print("Current size:%d" % comm_size)

def divide_file():  
    file_object = open("miniTwitter2.csv", 'rb')
    while True:  
        print(str(comm_rank)+"===============")
        chunk = file_object.readlines(chunksize) 
        if not chunk:  
              break  
        yield chunk  
    file_object.close()

if __name__ == "__main__": 
    part_term = []
    part_tweeters = Counter()
    part_topics = Counter()
    for files in divide_file():  
        print(comm_rank)
        if comm_rank == 0:
            rows_num = len(files)
            block = math.ceil(rows_num / comm_size)
            chunks = [files[i:i+block] for i in range(0, rows_num, block)]
        else:
            chunks = None
        start = MPI.Wtime()
        local_data = comm.scatter(chunks, root = 0)
        end = MPI.Wtime()
        time_diff = "%.2f" % (end - start)
        print('\nTime: %s s' % time_diff)

        local_data_list = [local_data[i:i+200] for i in range(0, len(local_data), 200)]

        for row_list in local_data_list:
            row = str(row_list).strip('[]')
            #Count the number of times a given term appears
            term_pat = re.compile('(\W)'+ term.lower() + '(\W)')
            part_term.append(len(term_pat.findall(str(row.lower()))))

            #Count the number of times for tweeters
            tweeters_pat = re.compile(r'(?<=@)\w+')
            part_tweeters.update(tweeters_pat.findall(str(row.lower())))

            #Count the number of times for topics
            topics_pat = re.compile(r'(?<=#)\w+')
            part_topics.update(topics_pat.findall(str(row.lower())))
      
        sum_term = comm.reduce(sum(part_term), root = 0, op = MPI.SUM) 
        tweeters = comm.reduce(part_tweeters, root = 0)
        topics = comm.reduce(part_topics, root = 0)

    time_end = MPI.Wtime()

    if comm_rank == 0:

        print('****************** \"%s\" %d occurrences ******************' % (term, sum_term))
        
        print('\n****************** Top 10 Tweeters ******************')
        i = 0
        for (name, occurrence) in Counter(tweeters).most_common(10):  
            i += 1          
            print('%d. @%s %d' % (i, name, occurrence))

        print('\n****************** Top 10 Topics ******************')
        i = 0
        for (topic, occurrence) in Counter(topics).most_common(10):  
            i += 1          
            print('%d. #%s %d' % (i, topic, occurrence))
        
        # Calculating the duration time.
        time_diff = "%.2f" % (time_end - time_start)
        print('\nTime: %s s' % time_diff)








