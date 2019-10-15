package org.deo.lock;

import org.deo.lock.jdbc.LockRequest;
import org.deo.lock.jdbc.SqlDistributedLockManager;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAcquireAndReleaseAllLocksEvenly() throws InterruptedException {
        SqlDistributedLockManager sqlDistributedLockManager = new SqlDistributedLockManager(getDatabaseConnection(), "LOCKS", Clock.systemDefaultZone());
        sqlDistributedLockManager.initializeDb();
        AtomicInteger lockCounter = sampleSqlLock();
        assertTrue(lockCounter.get() == 0);
    }

    private Connection getDatabaseConnection() {
        Properties connectionProps = new Properties();
        String sql_url = "jdbc:h2:mem:test";
        String sql_username = "SA";//"<put user id>";
        String sql_password = "";//"<password>";

        connectionProps.put("user", sql_username);
        connectionProps.put("password", sql_password);
        try {
            return DriverManager
                    .getConnection(sql_url, connectionProps);
        } catch (SQLException e) {
            throw new RuntimeException("Could not create SQL connection", e);
        }
    }

    /**
     * Race condition test for sample sql lock
     */
    AtomicInteger sampleSqlLock() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(6);

        AtomicInteger lockCounter = new AtomicInteger(0);
        startLockingSimulation(lockCounter);


        for (int i = 0; i < 20; i++) {
            executorService.submit(() -> {
                startLockingSimulation(lockCounter);
            });
        }
        executorService.awaitTermination(30, TimeUnit.SECONDS);
        executorService.shutdown();
        return lockCounter;
    }

    private void startLockingSimulation(AtomicInteger lockCounter) {
        final String currentThreadName = Thread.currentThread().getName();
        System.out.println(String.format("Thread started: %s", currentThreadName));
        SqlDistributedLockManager sqlDistributedLockManager = new SqlDistributedLockManager(getDatabaseConnection(), "LOCKS", Clock.systemDefaultZone());

        boolean acquired = sqlDistributedLockManager.acquire(new LockRequest("sample-lock", "currentThreadName", Duration.ofMinutes(1)));

        if (acquired) {
            lockCounter.incrementAndGet();
            System.out.println(String.format("Lock acquired! for thread: %s", currentThreadName));
            boolean released = sqlDistributedLockManager.forceRelease("sample-lock");
            if (released) {
                lockCounter.decrementAndGet();
                System.out.println(String.format("Lock released: %s with status %s", currentThreadName, released));
            }
        } else {
            System.out.println(String.format("Failed to acquire lock: %s", currentThreadName));
        }
    }
}
