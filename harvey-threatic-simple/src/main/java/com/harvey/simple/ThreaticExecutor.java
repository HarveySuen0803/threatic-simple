package com.harvey.simple;

import cn.hutool.core.collection.ConcurrentHashSet;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author harvey
 */
public class ThreaticExecutor {
    /**
     * The normal size of threads that will be kept in the pool.
     */
    private final int corePoolSize;
    
    /**
     * The maximum size of the pool.
     */
    private final int maxPoolSize;
    
    /**
     * The time to keep extra threads alive.
     */
    private final long keepAliveTime;
    
    /**
     * The time unit for the keep alive time.
     */
    private final TimeUnit keepAliveTimeUnit;
    
    /**
     * The queue to use for holding tasks before they are executed.
     */
    private static final BlockingQueue<Runnable> TASK_QUEUE = new LinkedBlockingQueue<>();
    
    /**
     * The set of workers that are not being used.
     */
    private static final Set<Worker> WORKER_SET = new ConcurrentHashSet<>();
    
    /**
     * The count of workers that are currently running.
     */
    private static final AtomicInteger WORKER_COUNT = new AtomicInteger(0);
    
    private static final int RUNNING = 0;
    private static final int STOPPED = 1;
    
    private static final String CORE_POOL = "corePool";
    private static final String MAX_POOL = "maxPool";
    
    /**
     * The state of the executor.
     */
    private static final AtomicInteger EXECUTOR_STATE = new AtomicInteger(0);
    
    /**
     * Lock held on access to workers set and related bookkeeping.
     * While we could use a concurrent set of some sort, it turns out
     * to be generally preferable to use a lock. Among the reasons is
     * that this serializes interruptIdleWorkers, which avoids
     * unnecessary interrupt storms, especially during shutdown.
     * Otherwise exiting threads would concurrently interrupt those
     * that have not yet interrupted. It also simplifies some of the
     * associated statistics bookkeeping of largestPoolSize etc. We
     * also hold mainLock on shutdown and shutdownNow, for the sake of
     * ensuring workers set is stable while separately checking
     * permission to interrupt and actually interrupting.
     */
    private static final ReentrantLock MAIN_LOCK = new ReentrantLock();
    
    /**
     * Wait condition to support awaitTermination.
     */
    private static final Condition MAIN_LOCK_CONDITION = MAIN_LOCK.newCondition();
    
    public ThreaticExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit keepAliveTimeUnit) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.keepAliveTimeUnit = keepAliveTimeUnit;
    }
    
    /**
     * Executes the given task sometime in the future.  The task
     * may execute in a new thread or in an existing pooled thread.
     * <p>
     * If the task cannot be submitted for execution, either because this
     * executor has been shutdown or because its capacity has been reached.
     */
    public void execute(Runnable task) throws RejectTaskException {
        checkTask(task);
        
        checkExecutor();
        
        executeTask(task);
    }
    
    /**
     * Execute the task immediately.
     * <p>
     * Todo: Optimize the logic among core threads, maximum threads, and task queues.
     */
    private void executeTask(Runnable task) throws RejectTaskException {
        // If the worker count is less than the core pool size, try to add a new worker.
        if (isCorePoolNotFull() && tryAddWorkerToCorePool(task)) {
            return;
        }
        
        // If the worker count is less than the maximum pool size, try to add a new worker.
        if (isMaxPoolNotFull() && tryAddWorkerToMaxPool(task)) {
            return;
        }
        
        // Add the task to the queue.
        MAIN_LOCK.lock();
        try {
            boolean isOffer = TASK_QUEUE.offer(task);
            if (!isOffer) {
                throw new RejectTaskException("The task cannot be added to the queue.");
            }
        } finally {
            MAIN_LOCK.unlock();
        }
    }
    
    private boolean tryAddWorkerToCorePool(Runnable task) throws RejectTaskException {
        return tryAddWorker(task, CORE_POOL);
    }
    
    private boolean tryAddWorkerToMaxPool(Runnable task) throws RejectTaskException {
        return tryAddWorker(task, MAX_POOL);
    }
    
    /**
     * Try to add a new worker to the pool.
     */
    private boolean tryAddWorker(Runnable task, String poolType) throws RejectTaskException {
        // Check if the worker count is less than the pool size.
        while (true) {
            checkExecutor();
            
            if (isCorePool(poolType) && isCorePoolFull()) {
                return false;
            } else if (isMaxPool(poolType) && isMaxPoolFull()) {
                return false;
            }
            
            WORKER_COUNT.incrementAndGet();
            
            break;
        }
        
        // Add the worker to the pool.
        MAIN_LOCK.lock();
        try {
            Worker worker = new Worker(task);
            
            Thread workerThread = worker.thread;
            if (workerThread == null) {
                return false;
            }
            workerThread.start();
            
            WORKER_SET.add(worker);
        } finally {
            MAIN_LOCK.unlock();
        }
        
        return true;
    }
    
    @EqualsAndHashCode
    private class Worker implements Runnable {
        /**
         * The first task to run.
         */
        private Runnable firstTask;
        
        /**
         * The thread that is running the worker.
         */
        private Thread thread;
        
        /**
         * The count of tasks that have been completed.
         */
        private final AtomicInteger completedTaskCount = new AtomicInteger(0);
        
        public Worker(Runnable firstTask) {
            this.firstTask = firstTask;
            this.thread = ThreadFactory.newThread(this);
        }
        
        @Override
        public void run() {
            try {
                runWorker(this);
            } catch (RejectTaskException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        
        private void runWorker(Worker worker) throws RejectTaskException, InterruptedException {
            checkWorker(worker);
            Runnable task = worker.firstTask;
            while (task != null) {
                checkExecutor();
                
                checkWorker(worker);
                
                task.run();
                
                task = tryGetTask();
                
                worker.completedTaskCount.incrementAndGet();
            }
            WORKER_SET.remove(worker);
            WORKER_COUNT.decrementAndGet();
        }
    }
    
    /**
     * Returns the next task to execute.
     */
    public Runnable tryGetTask() throws RejectTaskException {
        while (true) {
            checkExecutor();
            
            if (isTaskQueueEmpty()) {
                return null;
            }
            
            return TASK_QUEUE.poll();
        }
    }
    
    private static final ReentrantLock SHUTDOWN_LOCK = new ReentrantLock();
    
    public void shutdown() {
        SHUTDOWN_LOCK.lock();
        try {
            EXECUTOR_STATE.set(STOPPED);
            interruptAllWorkers();
        } finally {
            SHUTDOWN_LOCK.unlock();
        }
    }
    
    public List<Runnable> shutdownAndGetRemainingTaskList() {
        SHUTDOWN_LOCK.lock();
        List<Runnable> remainingTaskList = new ArrayList<>();
        try {
            EXECUTOR_STATE.set(STOPPED);
            interruptAllWorkers();
            
            if (isTaskQueueNotEmpty()) {
                TASK_QUEUE.drainTo(remainingTaskList);
            }
        } finally {
            SHUTDOWN_LOCK.unlock();
        }
        return remainingTaskList;
    }
    
    private void interruptAllWorkers() {
        for (Worker worker : WORKER_SET) {
            if (worker.thread.isInterrupted()) {
                continue;
            }
            worker.thread.interrupt();
        }
    }
    
    private void checkWorker(Worker worker) throws InterruptedException {
        if (worker == null) {
            throw new NullPointerException("The worker cannot be null");
        }
        
        Thread workerThread = worker.thread;
        if (workerThread.isInterrupted()) {
            throw new InterruptedException("The worker thread has been interrupted.");
        }
    }
    
    private void checkTask(Runnable task) {
        if (task == null) {
            throw new NullPointerException("The task cannot be null");
        }
    }
    
    private void checkExecutor() throws RejectTaskException {
        if (EXECUTOR_STATE.get() == STOPPED) {
            throw new RejectTaskException("The executor has been stopped.");
        }
    }
    
    private boolean isTaskQueueNotEmpty() {
        return !isTaskQueueEmpty();
    }
    
    private boolean isTaskQueueEmpty() {
        return TASK_QUEUE.isEmpty();
    }
    
    private boolean isPoolFull(boolean isCorePool) {
        if (isCorePool) {
            return isCorePoolFull();
        } else {
            return isMaxPoolFull();
        }
    }
    
    private boolean isPoolNotFull(boolean isCoreTask) {
        return !isPoolFull(isCoreTask);
    }
    
    private boolean isCorePoolFull() {
        return WORKER_COUNT.get() >= corePoolSize;
    }
    
    private boolean isCorePoolNotFull() {
        return !isCorePoolFull();
    }
    
    private boolean isMaxPoolFull() {
        return WORKER_COUNT.get() >= maxPoolSize;
    }
    
    private boolean isMaxPoolNotFull() {
        return !isMaxPoolFull();
    }
    
    private boolean isCorePool(String poolType) {
        return CORE_POOL.equals(poolType);
    }
    
    private boolean isMaxPool(String poolType) {
        return MAX_POOL.equals(poolType);
    }
}
