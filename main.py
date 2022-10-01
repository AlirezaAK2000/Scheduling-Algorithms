#!/usr/bin/env python

"""
taskset.py - parser for task set from JSON file
"""

from collections import defaultdict
from copy import deepcopy
import json
from re import M, S
import sys
import heapq
from tkinter.tix import TList
import matplotlib.pyplot as plt
import random




class TaskSetJsonKeys(object):
    # Task set
    KEY_TASKSET = "taskset"

    # Task
    KEY_TASK_ID = "taskId"
    KEY_TASK_PERIOD = "period"
    KEY_TASK_WCET = "wcet"
    KEY_TASK_DEADLINE = "deadline"
    KEY_TASK_OFFSET = "offset"
    KEY_TASK_SECTIONS = "sections"

    # Schedule
    KEY_SCHEDULE_START = "startTime"
    KEY_SCHEDULE_END = "endTime"

    # Release times
    KEY_RELEASETIMES = "releaseTimes"
    KEY_RELEASETIMES_JOBRELEASE = "timeInstant"
    KEY_RELEASETIMES_TASKID = "taskId"


class TaskSetIterator:
    def __init__(self, taskSet):
        self.taskSet = taskSet
        self.index = 0
        self.keys = iter(taskSet.tasks)

    def __next__(self):
        key = next(self.keys)
        return self.taskSet.tasks[key]


class TaskSet(object):
    def __init__(self, data):
        self.parseDataToTasks(data)
        self.buildJobReleases(data)

    def parseDataToTasks(self, data):
        taskSet = {}

        for taskData in data[TaskSetJsonKeys.KEY_TASKSET]:
            task = Task(taskData)

            if task.id in taskSet:
                print("Error: duplicate task ID: {0}".format(task.id))
                return

            if task.period < 0 and task.relativeDeadline < 0:
                print("Error: aperiodic task must have positive relative deadline")
                return

            taskSet[task.id] = task

        self.tasks = taskSet

    def buildJobReleases(self, data):
        jobs = []
        self.jobs_time = defaultdict(lambda : [])
        self.job_deadline = defaultdict(lambda: [])

        if TaskSetJsonKeys.KEY_RELEASETIMES in data:  # necessary for sporadic releases
            for jobRelease in data[TaskSetJsonKeys.KEY_RELEASETIMES]:
                releaseTime = float(jobRelease[TaskSetJsonKeys.KEY_RELEASETIMES_JOBRELEASE])
                taskId = int(jobRelease[TaskSetJsonKeys.KEY_RELEASETIMES_TASKID])

                job = self.getTaskById(taskId).spawnJob(releaseTime)
                jobs.append(job)
        else:
            self.scheduleStartTime = float(data[TaskSetJsonKeys.KEY_SCHEDULE_START])
            self.scheduleEndTime = float(data[TaskSetJsonKeys.KEY_SCHEDULE_END])
            for task in self:
                t = max(task.offset, self.scheduleStartTime)
                while t < self.scheduleEndTime:
                    job = task.spawnJob(t)
                    if job is not None:
                        jobs.append(job)
                    self.jobs_time[t].append(job)
                    self.job_deadline[job.deadline].append(job)
                    if task.period >= 0:
                        t += task.period  # periodic
                    else:
                        t = self.scheduleEndTime  # aperiodic

        self.jobs = jobs

    def __contains__(self, elt):
        return elt in self.tasks

    def __iter__(self):
        return TaskSetIterator(self)

    def __len__(self):
        return len(self.tasks)

    def getTaskById(self, taskId):
        return self.tasks[taskId]

    def printTasks(self):
        print("\nTask Set:")
        for task in self:
            print(task)

    def printJobs(self):
        print("\nJobs:")
        for task in self:
            for job in task.getJobs():
                print(job)

    @property
    def all_sections(self):
        sections = set()
        for task in self:
            for t in [i[0] for i in task.sections]:
                sections.add(t)
        return sections
            
            
    
class Task(object):
    def __init__(self, taskDict):
        self.id = int(taskDict[TaskSetJsonKeys.KEY_TASK_ID])
        self.period = float(taskDict[TaskSetJsonKeys.KEY_TASK_PERIOD])
        self.wcet = float(taskDict[TaskSetJsonKeys.KEY_TASK_WCET])
        self.relativeDeadline = float(
            taskDict.get(TaskSetJsonKeys.KEY_TASK_DEADLINE, taskDict[TaskSetJsonKeys.KEY_TASK_PERIOD]))
        self.offset = float(taskDict.get(TaskSetJsonKeys.KEY_TASK_OFFSET, 0.0))
        self.sections = taskDict[TaskSetJsonKeys.KEY_TASK_SECTIONS]

        self.lastJobId = 0
        self.lastReleasedTime = 0.0
        # self.sections_with_zero_interval = []
        # for section in self.sections:
        #     self.sections_with_zero_interval.append(section)
        #     self.sections_with_zero_interval.append([0,0])
        # self.sections_with_zero_interval = self.sections_with_zero_interval[:-1]
        self.jobs = []

    def getAllResources(self):
        return self.sections

    def spawnJob(self, releaseTime):
        if self.lastReleasedTime > 0 and releaseTime < self.lastReleasedTime:
            print("INVALID: release time of job is not monotonic")
            return None

        if self.lastReleasedTime > 0 and releaseTime < self.lastReleasedTime + self.period:
            print("INVDALID: release times are not separated by period")
            return None

        self.lastJobId += 1
        self.lastReleasedTime = releaseTime

        job = Job(self, self.lastJobId, releaseTime)

        self.jobs.append(job)
        return job

    def getJobs(self):
        return self.jobs

    def getJobById(self, jobId):
        if jobId > self.lastJobId:
            return None

        job = self.jobs[jobId - 1]
        if job.id == jobId:
            return job

        for job in self.jobs:
            if job.id == jobId:
                return job

        return None

    def getUtilization(self):
        return self.wcet / self.period

    def __str__(self):
        return "task {0}: (Φ,T,C,D,∆) = ({1}, {2}, {3}, {4}, {5})".format(self.id, self.offset, self.period, self.wcet,
                                                                          self.relativeDeadline, self.sections)


class Job(object):
    def __init__(self, task:Task, jobId, releaseTime):
        self.task = task
        self.jobId = jobId
        self.releaseTime = releaseTime
        self.is_completed = False
        self.remaining_time = self.task.wcet
        self.sections = deepcopy(self.task.sections)
        self.current_resource = 0
        self.deadline = releaseTime + self.task.relativeDeadline
        self.actual = 1/self.task.relativeDeadline
        self.priority = self.actual

    def getResourceHeld(self):
        '''the resources that it's currently holding'''
        return self.sections[self.current_resource][0] 
    def getRecourseWaiting(self):
        '''a resource that is being waited on, but not currently executing'''
        return self.sections[self.current_resource + 1:]
        
    def getRemainingSectionTime(self):
        return self.sections[self.current_resource][1]
    
        

    def execute(self, time):
        remaining = time 
        while remaining > 0:
            time_section = self.getRemainingSectionTime()
            if remaining >= time_section:
                self.sections[self.current_resource][1] = 0
                self.current_resource += 1
                remaining -= time_section
                if self.current_resource >= len(self.sections):
                    self.current_resource -= 1
                    self.is_completed = True
                return remaining
            else:
                self.sections[self.current_resource][1] -= remaining
                remaining = 0
        return remaining
                
            
        
    def executeToCompletion(self):
        return None
    
    def getPriority(self):
        return self.priority

    def setPriority(self, priority):
        self.priority = priority

    def resetPriority(self):
        self.priority = self.actual

    def isCompleted(self):
        return self.is_completed

    def __str__(self):
        return "[{0}:{1}] released at {2} -> deadline at {3}".format(self.task.id, self.jobId, self.releaseTime,
                                                                     self.deadline)
    def __le__(self, other):
        return 1/self.getPriority() <= 1/other.getPriority()
    
    def __lt__(self, other):
        return 1/self.getPriority() < 1/other.getPriority()

class PriorityQueue:
    
    def __init__(self) -> None:
        self.queue = []
        
    def insert(self, job: Job):
        heapq.heappush(self.queue, job)
        
    def pop(self) -> Job:
        return heapq.heappop(self.queue)
    
    def insertAll(self, jobs):
        for job in jobs:
            self.insert(job)

    def is_empty(self):
        return len(self.queue) == 0
    

class Scheduler:
    
    def __init__(self, taskset:TaskSet, mode = 'NPP', time_step = 1) -> None:
        self.taskset = taskset
        self.queue = PriorityQueue()
        self.mode = mode
        self.time_step = time_step
        
    def run(self):
        job_to_exec: Job = None
        interval = None
        intervals = []
        current_section = None
        missed_deadlines = []
        
        if self.mode == 'NPP':
            t = self.taskset.scheduleStartTime
            while t <= self.taskset.scheduleEndTime:
                jobs = self.taskset.jobs_time[t]
                self.queue.insertAll(jobs )
                
                if not self.queue.is_empty():
                    potential_job = self.queue.pop()
                    
                    if job_to_exec == None:
                        job_to_exec = potential_job
                        interval = [t,t,
                            False, #caused premption
                            False, #completed
                            job_to_exec.jobId,
                            job_to_exec.task.id,
                            job_to_exec.getResourceHeld()
                        ]
                    
                    elif job_to_exec.getResourceHeld() == 0 and \
                        potential_job.getPriority() > job_to_exec.getPriority():
                        self.queue.insert(job_to_exec)
                        if interval[0] != interval[1]:
                            intervals.append(interval)
                        job_to_exec = potential_job
                        interval = [
                            t,
                            t,
                            True, #caused premption
                            False, #completed
                            job_to_exec.jobId,
                            job_to_exec.task.id,
                            job_to_exec.getResourceHeld()
                        ]
                    elif interval[6] != current_section and current_section != None:
                        self.queue.insert(potential_job)
                        if interval[0] != interval[1]:
                            intervals.append(interval)
                        self.queue.insert(job_to_exec)
                        job_to_exec = self.queue.pop()
                        interval = [t,t,
                            False, #caused premption
                            False, #completed
                            job_to_exec.jobId,
                            job_to_exec.task.id,
                            job_to_exec.getResourceHeld()
                        ]
                    else:
                        self.queue.insert(potential_job)
                        
                elif interval[6] != current_section and current_section != None and job_to_exec != None:
                    if interval[0] != interval[1]:
                        intervals.append(interval)
                    self.queue.insert(job_to_exec)
                    job_to_exec = None
                    continue        
                                
                    
                if job_to_exec != None:  
                    if job_to_exec.deadline < t:
                        missed_deadlines.append(job_to_exec)
                    remaining = job_to_exec.execute(self.time_step)
                    interval[1] = interval[1] + self.time_step - remaining
                    
                    if job_to_exec.isCompleted():
                        interval[3] = True
                        if interval[0] != interval[1]:
                            intervals.append(interval)
                        job_to_exec = None
                    else:
                        current_section = job_to_exec.getResourceHeld()  
                        
                        
                    
                    t += self.time_step - remaining
                else:
                    t += self.time_step
                
        elif self.mode == 'HLP':
            # calculating resources' priorities
            res_priorities = defaultdict(lambda : -1)
            
            for job in self.taskset.jobs:
                job_priority = job.getPriority()
                resources = job.task.sections
                for res in resources:
                    res_priorities[res[0]] = max(res_priorities[res[0]] , job_priority)
            del res_priorities[0]
            # running the scheduler
            
            t = self.taskset.scheduleStartTime
            while t <= self.taskset.scheduleEndTime:
                jobs = self.taskset.jobs_time[t]
                self.queue.insertAll(jobs)
                
                if not self.queue.is_empty():
                    potential_job = self.queue.pop()
                    
                    if job_to_exec == None:
                        job_to_exec = potential_job
                        interval = [t,t,
                            False, #caused premption
                            False, #completed
                            job_to_exec.jobId,
                            job_to_exec.task.id,
                            job_to_exec.getResourceHeld()
                        ]
                    
                    elif job_to_exec.getResourceHeld() == 0 and \
                        potential_job.getPriority() > job_to_exec.getPriority() or \
                            (potential_job.getPriority() > res_priorities[job_to_exec.getResourceHeld()]
                             and not (job_to_exec.getResourceHeld() == 0)):
                        self.queue.insert(job_to_exec)
                        if interval[0] != interval[1]:
                            intervals.append(interval)
                        job_to_exec = potential_job
                        interval = [
                            t,
                            t,
                            True, #caused premption
                            False, #completed
                            job_to_exec.jobId,
                            job_to_exec.task.id,
                            job_to_exec.getResourceHeld()
                        ]
                    elif interval[6] != current_section and current_section != None:
                        self.queue.insert(potential_job)
                        if interval[0] != interval[1]:
                            intervals.append(interval)
                        self.queue.insert(job_to_exec)
                        job_to_exec = self.queue.pop()
                        interval = [t,t,
                            False, #caused premption
                            False, #completed
                            job_to_exec.jobId,
                            job_to_exec.task.id,
                            job_to_exec.getResourceHeld()
                        ]
                    else:
                        self.queue.insert(potential_job)
                        
                elif interval[6] != current_section and current_section != None and job_to_exec != None:
                    if interval[0] != interval[1]:
                        intervals.append(interval)
                    self.queue.insert(job_to_exec)
                    job_to_exec = None
                    continue                        
                                
                    
                if job_to_exec != None:
                    if job_to_exec.deadline < t:
                        missed_deadlines.append(job_to_exec)  
                    remaining = job_to_exec.execute(self.time_step)
                    interval[1] = interval[1] + self.time_step - remaining
                    
                    if job_to_exec.isCompleted():
                        interval[3] = True
                        if interval[0] != interval[1]:
                            intervals.append(interval)
                        job_to_exec = None
                    else:
                        current_section = job_to_exec.getResourceHeld()  
                        
                        
                    
                    t += self.time_step - remaining
                else:
                    t += self.time_step
        
        if self.mode == "PIP":
            resources_waiting = dict()
            
            for sec in self.taskset.all_sections:
                resources_waiting[sec] = None
            
            t = self.taskset.scheduleStartTime
            print("=====================")
            while t <= self.taskset.scheduleEndTime:
                jobs = self.taskset.jobs_time[t]
                self.queue.insertAll(jobs)
                print("=======")
                print(job_to_exec)
                print(interval)
                
                if not self.queue.is_empty():
                    potential_job = self.queue.pop()
                    
                    if job_to_exec == None:
                        
                        if potential_job.getResourceHeld() != 0 and resources_waiting[potential_job.getResourceHeld()] != None and resources_waiting[potential_job.getResourceHeld()] != potential_job:
                            self.queue.queue.remove(resources_waiting[potential_job.getResourceHeld()]) 
                            tmp = resources_waiting[potential_job.getResourceHeld()]
                            tmp.setPriority(potential_job.getPriority())
                            heapq.heapify(self.queue.queue)
                            self.queue.insert(potential_job)
                            potential_job = tmp
                        elif potential_job.getResourceHeld() != 0:
                            resources_waiting[potential_job.getResourceHeld()] = potential_job
                            
                        
                        job_to_exec = potential_job
                        interval = [t,t,
                            False, #caused premption
                            False, #completed
                            job_to_exec.jobId,
                            job_to_exec.task.id,
                            job_to_exec.getResourceHeld()
                        ]
                        
                    elif potential_job.getPriority() > job_to_exec.getPriority():
                        prev_job = job_to_exec
                        self.queue.insert(job_to_exec)
                        
                            
                        if potential_job.getResourceHeld() != 0 and resources_waiting[potential_job.getResourceHeld()] != None:
                            self.queue.queue.remove(resources_waiting[potential_job.getResourceHeld()]) 
                            tmp = resources_waiting[potential_job.getResourceHeld()]
                            tmp.setPriority(potential_job.getPriority())
                            heapq.heapify(self.queue.queue)
                            self.queue.insert(potential_job)
                            potential_job = tmp
                        elif potential_job.getResourceHeld() != 0:
                            resources_waiting[potential_job.getResourceHeld()] = potential_job    
                        
                        job_to_exec = potential_job
                        if job_to_exec != prev_job:
                            if interval[0] != interval[1]:
                                intervals.append(interval)
                            interval = [t,t,
                                True, #caused premption
                                False, #completed
                                job_to_exec.jobId,
                                job_to_exec.task.id,
                                job_to_exec.getResourceHeld()
                            ]
                    elif interval[6] != current_section and current_section != None and job_to_exec != None:
                        if interval[0] != interval[1]:
                            intervals.append(interval)
                        job_to_exec.resetPriority()
                        self.queue.insert(job_to_exec)
                        self.queue.insert(potential_job)
                        resources_waiting[interval[6]] = None
                        
                        job_to_exec = None
                        potential_job = self.queue.pop()
                        
                        if potential_job.getResourceHeld() != 0 and resources_waiting[potential_job.getResourceHeld()] != None:
                            self.queue.queue.remove(resources_waiting[potential_job.getResourceHeld()]) 
                            tmp = resources_waiting[potential_job.getResourceHeld()]
                            tmp.setPriority(potential_job.getPriority())
                            heapq.heapify(self.queue.queue)
                            self.queue.insert(potential_job)
                            potential_job = tmp
                        elif potential_job.getResourceHeld() != 0:
                            resources_waiting[potential_job.getResourceHeld()] = potential_job
                            
                        
                        job_to_exec = potential_job
                        interval = [t,t,
                            False, #caused premption
                            False, #completed
                            job_to_exec.jobId,
                            job_to_exec.task.id,
                            job_to_exec.getResourceHeld()
                        ]
                    else:
                        self.queue.insert(potential_job )        
                elif interval[6] != current_section and current_section != None and job_to_exec != None:
                    if interval[0] != interval[1]:
                        intervals.append(interval)
                    job_to_exec.resetPriority()
                    self.queue.insert(job_to_exec)
                    
                    resources_waiting[interval[6]] = None
                    
                    job_to_exec = None
                    continue
                        
                    
                    
                if job_to_exec != None:  
                    if job_to_exec in self.taskset.job_deadline[t] and job_to_exec not in missed_deadlines:
                        missed_deadlines.append(job_to_exec)
                    remaining = job_to_exec.execute(self.time_step)
                    interval[1] = interval[1] + self.time_step - remaining
                    
                    if job_to_exec.isCompleted():
                        interval[3] = True
                        if interval[0] != interval[1]:
                            intervals.append(interval)
                        job_to_exec.resetPriority()
                        resources_waiting[interval[6]] = None
                        job_to_exec = None
                    else:
                        current_section = job_to_exec.getResourceHeld()  
                        
                    
                    t += self.time_step - remaining
                else:
                    t += self.time_step
        
        
        for inter in intervals:
            print(
                "interval [{},{}]: task {}, job {} (completed: {}, preempted previous: {}, section: {}) ".format(
                  inter[0],
                  inter[1],
                  inter[5],
                  inter[4],
                  inter[3],
                  inter[2],
                  inter[6]  
                )
            )    
        print("Validating the schedule")
        if len(missed_deadlines) == 0:
            print("no WCETs are missed")
            print("This schedule is feasible")
        else:
            for job in missed_deadlines:
                print(job)
            print("Are missed !!!")    
        self.plot_schedule(intervals)
                
                
    def plot_schedule(self , intervals):
        task_interval = defaultdict(lambda : [])
        section_color = dict()
        for interval in intervals:
            if interval[6] not in  section_color.keys():
                section_color[interval[6]] = "#"+''.join([random.choice('AB0123456789') for i in range(3)])

            task_interval[interval[5]].append([[interval[0] , interval[1] - interval[0]],section_color[interval[6]]])
            
        
        fig, gnt = plt.subplots()
                
        gnt.set_ylim(0, 100)
        
        gnt.set_xlim(0, self.taskset.scheduleEndTime)
        
        gnt.set_xlabel('Time Steps')
        gnt.set_ylabel('Task')
        
        tmp = 100 // (len(self.taskset.tasks) + 2)
        ticks = [100 - i* tmp for i in range(1,len(self.taskset.tasks) + 1)]
        gnt.set_yticks(ticks)
        gnt.set_yticklabels([ i + 1 for i in range(len(self.taskset.tasks))])
        
        gnt.grid(True)
        
        for i, task_id in enumerate(sorted(task_interval.keys())):        
            height = 100 - (i + 3/2) * tmp 
            gnt.broken_barh([a[0] for a in task_interval[task_id]], ( height, tmp - 2),
                                    facecolors =[a[1] for a in task_interval[task_id]])
            
        plt.show()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        mode = sys.argv[2]
    else:
        file_path = "taskset.json"

    with open(file_path) as json_data:
        data = json.load(json_data)

    taskSet = TaskSet(data)

    taskSet.printTasks()
    taskSet.printJobs()

    Scheduler(taskSet,mode=mode).run()