Sentence Transformers Scaled on a 16-core machine.
Scaling Architecture - 3 processes of Sentence Tranformer are spawned each using 5 workers to increase the throughput of Sentence Transformers.
15 workers - sentence transformer is able to do one batch of 1000 urls in 10 minutes.
Splitting workers in multiple processes - increases throughput to 3 batches in 10 minute
