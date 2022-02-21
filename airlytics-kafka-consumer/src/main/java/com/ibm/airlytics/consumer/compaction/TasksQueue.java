package com.ibm.airlytics.consumer.compaction;

import java.util.PriorityQueue;

public class TasksQueue {
    public static class Task implements Comparable<Task>{

        int shard;
        String day;
        Double score;
        String subFolder;
        long age; //in days
        double originalScore;

        public Task(int shard, String day, Double score, long age, double originalScore) {
            this.shard = shard;
            this.day = day;
            this.score = score;
            this.age = age;
            this.subFolder = CompactionConsumer.getShardDayFolder(shard, day);
            this.originalScore = originalScore;
        }

        @Override
        public int compareTo(Task o) {
            return o.score.compareTo(score);
        }

        public String getDay() {
            return day;
        }

        public int getShard() {
            return shard;
        }

        public long getAge() {
            return age;
        }

        public Double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }


        public String getSubFolder() {
            return subFolder;
        }

        public double getOriginalScore() {
            return originalScore;
        }

        public String toString() {
            return "shard = " + shard + ", day = " + day + ", final score = " + score + ", age = " + age + ", original score = " + originalScore;
        }
    }

    private PriorityQueue<Task> queue = new PriorityQueue<>();

    public void clearQueue() {
        queue.clear();
    }

    public void updateTaskScore(Task task, Double newScore) {
        queue.remove(task);
        task.setScore(newScore);
        queue.add(task);
    }

    public Task getFirstTask() {
        return queue.poll();
    }

    public void addTask(int shard, String day, Double score, long age, Double originalScore) {
        queue.add(new Task(shard, day, score, age, originalScore));
    }

    public void addTask(Task newTask) {
        queue.add(newTask);
    }


    public static void main(String[] args) {
        TasksQueue q = new TasksQueue();
        q.addTask(1,"a", 3.3, 2, 3.0);
        q.addTask(2,"b", 300.1, 3, 2.1);
        q.addTask(3,"c", 1.2, 4, 1.1);
        q.addTask(4,"d", 13.5, 5, 3.3);
        q.addTask(5,"e", 3.4, 6, 2.2);
        q.addTask(6,"f", 20.0, 7, 19.1);

        for (int i=0; i<6; ++i) {
            System.out.println(q.getFirstTask().getScore());
        }
    }

    public int size() {
        return queue.size();
    }
}
