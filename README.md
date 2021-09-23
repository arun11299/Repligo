# Repligo
## Database Replication System

Apache 2.0 License

### What is it for ?
The final purpose of this tool is to replicate data between two Database systems (cluster/standalone) which are residing on two different nodes/zones/region.
For eg: You have a standalone Mongo on one site and you need to replicate the data to another independent site (i.e. two separate clusters). You could have any of the following requirement to sync the data between these two systems:
1. Stream data as it happens unidirectionally or bidirectionally.
2. Stream data using a checkpoint i.e so as to sync the missed changes as well.
3. Full database reload and sync.

Now imagine doing this for Redis, Postgres etc. ?

The purpose of this software is able to make use of the functionalities provided by the Database itself to make this happen without the need to install anyother components in your stack.
