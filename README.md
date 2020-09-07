### Postgresql WAL Reader

** still a work-in-progress ** 


This is a test application that can use the wal2json or test_decoder reader to read and process the Postresql WAL logs. The idea is to generate performance statistics for record processing speeds an to generate some information about the records we receive in the replication stream.  Specifically we want to know how many records are skipped (wrong database/schema/table).  We also want to gather information regarding the complexity of the transactions that we're actually going to process.  

Support is mostly in place for the test_decoder, wal2json coming soon.   

The program will (eventually) generate a summary when you break out with CTRL-c. 

- [x] catch CTRL-c
- [ ] statistics for schemas/tables
- [ ] support for wal2json
- [ ] statistics regarding transaction complexity.