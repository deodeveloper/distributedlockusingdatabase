package org.deo.lock.jdbc;

import java.sql.*;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;


public class SqlDistributedLockManager {
    private final SqlQueries sqlQueries;
    private final Clock clock;
    private final Connection connection;

    public SqlDistributedLockManager(
            Connection connection, String tableName, Clock clock) {
        this.clock = Objects.requireNonNull(clock, "Expected non null clock");
        this.sqlQueries = new SqlQueries(tableName);
        this.connection = Objects.requireNonNull(connection);
    }

    public boolean acquire(LockRequest lockRequest) {
        Instant now = now();
        return updateReleasedLock(lockRequest, now)
                || insertLock(lockRequest, now);
    }

    public boolean acquireOrProlong(LockRequest lockRequest) {
        Instant now = now();
        return updateAcquiredOrReleasedLock(lockRequest, now)
                || insertLock(lockRequest, now);
    }

    public boolean forceAcquire(LockRequest lockRequest) {
        Instant now = now();
        return updateLockById(lockRequest, now)
                || insertLock(lockRequest, now);
    }

    private boolean updateReleasedLock(LockRequest lockRequest, Instant now) {
        String lockId = lockRequest.getLockId();
        Instant expiresAt = expiresAt(now, lockRequest.getDuration());
        try (PreparedStatement statement = getStatement(sqlQueries.updateReleasedLock())) {
            statement.setString(1, lockRequest.getOwnerId());
            statement.setTimestamp(2, timestamp(now));
            setupOptionalTimestamp(statement, 3, expiresAt);
            statement.setString(4, lockId);
            statement.setTimestamp(5, timestamp(now));
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("SQL Error when updating a lock: " + lockId, e);
        }
    }

    private boolean updateAcquiredOrReleasedLock(LockRequest lockRequest, Instant now) {
        String lockId = lockRequest.getLockId();
        Instant expiresAt = expiresAt(now, lockRequest.getDuration());
        try (PreparedStatement statement = getStatement(sqlQueries.updateAcquiredOrReleasedLock())) {
            statement.setString(1, lockRequest.getOwnerId());
            statement.setTimestamp(2, timestamp(now));
            setupOptionalTimestamp(statement, 3, expiresAt);
            statement.setString(4, lockId);
            statement.setString(5, lockRequest.getOwnerId());
            statement.setTimestamp(6, timestamp(now));
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("SQL Error when updating a lock: " + lockId, e);
        }
    }

    private boolean updateLockById(LockRequest lockRequest, Instant now) {
        String lockId = lockRequest.getLockId();
        Instant expiresAt = expiresAt(now, lockRequest.getDuration());
        try (PreparedStatement statement = getStatement(sqlQueries.updateLockById())) {
            statement.setString(1, lockRequest.getOwnerId());
            statement.setTimestamp(2, timestamp(now));
            setupOptionalTimestamp(statement, 3, expiresAt);
            statement.setString(4, lockId);
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("SQL Error when updating a lock: " + lockId, e);
        }
    }

    private boolean insertLock(LockRequest lockRequest, Instant now) {
        String lockId = lockRequest.getLockId();
        Instant expiresAt = expiresAt(now, lockRequest.getDuration());
        try (PreparedStatement statement = getStatement(sqlQueries.insertLock())) {
            statement.setString(1, lockId);
            statement.setString(2, lockRequest.getOwnerId());
            statement.setTimestamp(3, timestamp(now));
            setupOptionalTimestamp(statement, 4, expiresAt);
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            if (!e.getMessage().toLowerCase().contains("duplicate")) {
                throw new IllegalStateException("SQL Error when inserting a lock: " + lockId, e);
            }
            return false;
        }
    }

    public boolean release(String lockId, String ownerId) {
        try (PreparedStatement statement = getStatement(sqlQueries.deleteAcquiredByIdAndOwnerId())) {
            statement.setString(1, lockId);
            statement.setString(2, ownerId);
            statement.setTimestamp(3, timestamp(now()));
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("Could not delete lock: " + lockId, e);
        }
    }

    public boolean forceRelease(String lockId) {
        try (PreparedStatement statement = getStatement(sqlQueries.deleteAcquiredById())) {
            statement.setString(1, lockId);
            statement.setTimestamp(2, timestamp(now()));
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("Could not delete lock: " + lockId, e);
        }
    }

    public boolean forceReleaseAll() {
        try (PreparedStatement statement = getStatement(sqlQueries.deleteAll())) {
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("Could not delete all locks", e);
        }
    }

    private Instant now() {
        return clock.instant();
    }

    private Instant expiresAt(Instant now, Duration duration) {
        if (duration == null) {
            return null;
        }
        return now.plus(duration);
    }

    private void setupOptionalTimestamp(PreparedStatement statement, int index, Instant instant)
            throws SQLException {
        if (instant != null) {
            statement.setTimestamp(index, timestamp(instant));
        } else {
            statement.setNull(index, Types.TIMESTAMP);
        }
    }

    public void initializeDb() {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sqlQueries.createLocksTable());
            // TODO: Setup indexes
        } catch (SQLException e) {
            try (PreparedStatement statement = connection
                    .prepareStatement(sqlQueries.checkTableExits())) {
                statement.executeQuery();
                // if no error then table exists
            } catch (SQLException checkException) {
                throw new IllegalStateException("Could not initialize locks table", e);
            }
        }
    }

    private Timestamp timestamp(Instant instant) {
        return new Timestamp(instant.toEpochMilli());
    }

    private PreparedStatement getStatement(String sql) {
        try {
            return connection.prepareStatement(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("Could not create SQL statement", e);
        }
    }
}
