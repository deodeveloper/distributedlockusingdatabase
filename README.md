# distributed lock using sql database
Distributed lock using database (works with h2, postgres, sql server, mysql, oracle)
Remember to add appropriate jdbc driver in the classpath(like using maven dependency) to talk to the database. 

Simple API - cross-compiled to work with jdk 8+
Usage - 
```java
  SqlDistributedLockManager sqlDistributedLockManager = new SqlDistributedLockManager(getDatabaseConnection(), 
        "LOCKS", Clock.systemDefaultZone());
        // acquire(lockName, lockOwnerId, DurationOfLock <for locksforever put a large duration>)
  boolean acquired = sqlDistributedLockManager.acquire(new LockRequest("sample-lock", "userId", Duration.ofMinutes(1)));
  if(acquired){
  // do your thing
  }
  //finally release the lock
  sqlDistributedLockManager.release("sample-lock", "userId")
```
Please create the table afront before using the api. 
Please add indexing on columns - ACQUIRED_BY, EXPIRES_AT
DataBase table schema to be created -
```sql
      create table LOCKS
      (
      	ID varchar(512) not null
      		constraint LOCKS_pk
      			primary key nonclustered,
      	ACQUIRED_BY varchar(512) not null,
      	ACQUIRED_AT datetime2 not null,
      	EXPIRES_AT datetime2 not null
      )
      -- add indexing on ACQUIRED_BY, EXPIRES_AT
      go
```
