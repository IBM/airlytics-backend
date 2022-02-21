package com.ibm.airlytics.retentiontrackerqueryhandler.scheduling;

import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import org.springframework.scheduling.TaskScheduler;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ScheduledFuture;

import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

@Component
public class ScheduleTaskService {

    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(ScheduleTaskService.class.getName());

    private class TaskToSchedule {
        ScheduledFuture<?> scheduledTask;
        CronTrigger trigger;
        public TaskToSchedule(ScheduledFuture<?> scheduledTask, CronTrigger trigger) {
            this.scheduledTask=scheduledTask;
            this.trigger=trigger;
        }
    }
    // Task Scheduler
    TaskScheduler scheduler;

    // A map for keeping scheduled tasks
    Map<String, TaskToSchedule> jobsMap = new HashMap<>();

    public ScheduleTaskService(TaskScheduler scheduler) {
        this.scheduler = scheduler;
    }


    // Schedule Task to be executed every night at 00 or 12 am
    public void addTaskToScheduler(String id, Runnable task, String trigger) {
        CronTrigger cronTrigger = new CronTrigger(trigger, TimeZone.getTimeZone("UTC"));
        ScheduledFuture<?> scheduledTask = scheduler.schedule(task, cronTrigger);

        jobsMap.put(id, new TaskToSchedule(scheduledTask, cronTrigger));
    }

    // Remove scheduled task
    public void removeTaskFromScheduler(String id) {
        TaskToSchedule task = jobsMap.get(id);
        if (task == null) return;

        ScheduledFuture<?> scheduledTask = task.scheduledTask;
        if(scheduledTask != null) {
            scheduledTask.cancel(false);
            jobsMap.put(id, null);
        }
    }

    public void updateSchedulingTask(String id, Runnable task, String trigger) {
        TaskToSchedule taskToSchedule = jobsMap.get(id);
        if (taskToSchedule == null) return;
        String oldTrigger = taskToSchedule.trigger.getExpression();
        if (!trigger.equals(oldTrigger)) {
            //trigger has changed
            logger.info("scheduled task for "+id+" timing has changed from "+oldTrigger+" to:"+trigger);
            removeTaskFromScheduler(id); //should cancel the task
            addTaskToScheduler(id, task, trigger);
        }
    }

    // A context refresh event listener
    @EventListener({ ContextRefreshedEvent.class })
    void contextRefreshedEvent() {
        // Get all tasks from DB and reschedule them in case of context restarted
    }
}
