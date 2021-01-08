
Entity Enhancer Module is scaled using Bulk Processor.
Current Batch Size is 1000
Concurrent Bulk Upload is not being done as present.
Each Worker Thread in Thread Pool Maintain its own copy of Bulk Processor using Thread Local.
Sharing Bulk Processor Among Multiple Thread Cause Issues.
