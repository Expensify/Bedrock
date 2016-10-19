Bedrock/sqlitecluster
======================
A general purpose clustered SQL database designed for embedding into servers to achieve high fault-tolerance and seamless failover/recovery.  Consists of two primary classes:

- **[SQLite](SQLite.html)** - A simple wraper around the SQLite embedded database that adds journalling.  This journalling is what allows SQLiteNode to determine the precise state of the database and compare it with peers, as well as synchronize across any missing changes.

- **[SQLiteNode](SQLiteNode.html)** - One node in the clustered SQLite database.  Wraps SQLite with the necessary functionality to connect with peers, synchronize to a common state, elect a master, and forward commands to the master for processing.
