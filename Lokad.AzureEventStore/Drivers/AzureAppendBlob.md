This document outlines the storage architecture and technical considerations
involved in Azure-based event storage.

# Design rationale

An individual Azure Append Blob supports multi-writer append operations with the
same lock-free pattern used by our system: if someone else wrote to the stream 
since the last time you read from it, then your write attempt will be rejected. 

This makes a single-blob implementation of an `IStorageDriver` very 
straightforward. 

However, there is a snag: a single blob only supports a limited number of 
appends, and that number is surprisingly low (only 50000). This leads us to
using multiple blobs to save a single stream, which creates several minor
challenges: 

 - Positions are global offsets. A mapping solution is needed to map positions
   to offsets within the relevant blobs. This is achieved by storing the 
   starting position of all blobs and performing a binary search. 
 - The reject-if-outdated property must be guaranteed even at the point when
   appends move from one block to the next. This is achieved by waiting until
   appends fail due to "too many appends" before starting the next block.
 - The creation of a new blob can occur at runtime (and not only at install
   time) and therefore must support concurrency. Luckily, there is a simple
   pattern to implement CreateIfNotExists.

# Event stream format

Events are stored one after another. The following fields are present: 

   ========----------------====(..)====----------------========
    size    key             content     checksum        size
	ushort  uint            byte[]      uint            ushort
	     
Content size is always a multiple of 8, so the size fields (at the beginning
and end of the object) contain that size divided by 8. The individual offsets 
are as follows: 

    size      0
	key       2
	content   6
	checksum  6 + size * 8
	size     10 + size * 8

Total size of an event is: 

    12 + size * 8

The maximum size of the event contents is approximately 512kB.