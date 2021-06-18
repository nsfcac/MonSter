import json

with open('./data/jobs.json') as f:
    job_data = json.load(f)

job = job_data['jobs'][0]

# print(job)
job_attribute = list(job.keys())
# print(job_attribute)
column_names = ('job_id', 'array_job_id', 'name', 'job_state', 'user_id', 'user_name', 'group_id', 'cluster', 'partition', 'command', 'current_working_directory', 'batch_host', 'nodes', 'node_count', 'cpus', 'tasks', 'tasks_per_node', 'cpus_per_task', 'memory_per_node', 'memory_per_cpu', 'priority', 'time_limit', 'deadline', 'submit_time', 'preempt_time', 'suspend_time', 'eligible_time', 'start_time', 'end_time', 'resize_time', 'restart_cnt', 'exit_code', 'derived_exit_code')
column_types = ['TEXT', 'INT', 'TEXT', 'TEXT', 'INT', 'TEXT', 'INT', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 'INT', 'INT[]', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'TEXT', 'TEXT']
        
print(column_names.index('end_time'))
print(len(column_types))
