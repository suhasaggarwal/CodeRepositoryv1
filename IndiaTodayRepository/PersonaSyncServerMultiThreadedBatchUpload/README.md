Persona Sync Server Multi Threaded Processing of Cookie Batches.
Cookie Batches are Serialised tO Disk using FileWriter.
Another version uses Google Guava to minimise Disk I/O Latencies so that Batches are written with minimum Latencies to the Disk.
