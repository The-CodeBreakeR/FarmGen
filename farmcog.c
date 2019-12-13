/* DONE targets (2nd phase):
   -change the return type of task-gen, and process functions to a better defined structure (containing the task itself and the additional/config info) [milestone 3]
   -change the data structure of the task itself to something better than an array of ints [milestone 3]
   -add analysis structure and possibility for receiver to send feedbacks from processed results to center [milestone 3 & 4]
   -add dummy memory to tasks and result to check its effects on performance and communication [milestone 5]
   	*with dummy memory and estimated elapsed time we can create as complicated examples for the system as needed
	*complexity comes from different memory requirement and runtime since logic should be maintained by user
	*therefore this milestone is done
   -try to fetch out parameters from the center/workers functions and add them to configuration (separate policy from mechanism) [milestone 4]
   	*send_analysis_to_center
	*algorithm pushing, pulling, combination, combthenpull
   -python code to generate them all [milestone 2]
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <stdbool.h>

/*
   tags
0: regular task (center -> servers)
1: regular result (servers -> receiver)
2: emergency task request (servers -> center)
3: update center (receiver -> center)
4: finish (center -> all , servers -> receiver)
5: regular analysis (receiver -> center)
 */

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) > (y)) ? (y) : (x))

typedef enum {INITIAL, MIDDLE, FINAL} Stage;

/*
push: center pushes job to servers based on information it has
pull: servers ask for new job
combination: both pull and push (pull is priority)
combthenpull: starts with a combination but final stage is only pull
 */
typedef enum {PUSH, PULL, COMBINATION, COMBTHENPULL} SchedulingAlgorithm;

/*[[[cog
import cog
file = open("farm.config", "r")
lines = file.read().splitlines()

initial_center_memo_size = False
for line in lines:
	if line.startswith('initial_center_memo_size'):
		cog.outl("const int %s;" % line)
		initial_center_memo_size = True
		break
if not initial_center_memo_size:
		cog.outl("const int initial_center_memo_size = 100000;")    

initial_receiver_memo_size = False
for line in lines:
	if line.startswith('initial_receiver_memo_size'):
		cog.outl("const int %s;" % line)
		initial_receiver_memo_size = True
		break
if not initial_receiver_memo_size:
	cog.outl("const int initial_receiver_memo_size = 100000;")

period_adjuster_to_update_center = False
for line in lines:
	if line.startswith('period_adjuster_to_update_center'):
		cog.outl("const int %s;" % line)
		period_adjuster_to_update_center = True
		break
if not period_adjuster_to_update_center:
	cog.outl("const int period_adjuster_to_update_center = 1;")

send_analysis_to_center = False
for line in lines:
	if line.startswith('send_analysis_to_center'):
		cog.outl("const bool %s;" % line)
		send_analysis_to_center = True
		break
if not send_analysis_to_center:
	cog.outl("const bool send_analysis_to_center = true;")

scheduling_algorithm = False
for line in lines:
	if line.startswith('scheduling_algorithm'):
		cog.outl("const SchedulingAlgorithm %s;" % line)
		scheduling_algorithm = True
		break
if not scheduling_algorithm:
	cog.outl("const SchedulingAlgorithm scheduling_algorithm = COMBINATION;")

new_problem = False
for line in lines:
	if line.startswith('new_problem') and line.endswith('true'):
		new_problem = True
		break

if new_problem:
	for line in lines:
		if line.startswith('Task'):
			filepath = line.split()[-1]
			cog.outl("\n// comes from %s" % filepath)
			codefile = open(filepath, "r")
			code = codefile.read()
			cog.outl("%s" % code)
			break
else:
	cog.out("""
// DEFAULT: sample Task struct with estimated runtime and dummy data
typedef struct Task 
{
	int estimated_time;
	int task_num;
	// data inside is not important for the logic
	// it is just to check the performance of communication with big size tasks
	int dummy_data[];
} Task;
	""")

if new_problem:
	for line in lines:
		if line.startswith('Result'):
			filepath = line.split()[-1]
			cog.outl("// comes from %s" % filepath)
			codefile = open(filepath, "r")
			code = codefile.read()
			cog.outl("%s" % code)
			break
else:
	cog.out("""
// DEFAULT: sample Result struct with dummy data
typedef struct Result
{
	bool invalid_task;
	// data inside is not important for the logic
	// it is just to check the performance of communication with big size tasks
	int dummy_data[];
} Result;
	""")

if new_problem:
	for line in lines:
		if line.startswith('Analysis'):
			filepath = line.split()[-1]
			cog.outl("// comes from %s" % filepath)
			codefile = open(filepath, "r")
			code = codefile.read()
			cog.outl("%s" % code)
			break
else:
	cog.out("""
// DEFAULT: sample Analysis struct
typedef struct Analysis
{
	bool means_nothing; // always true here :)
} Analysis;
	""")
]]]*/
//[[[end]]]
// includes task and more info which is returned by generate_task function
typedef struct TaskPack
{
	bool no_more_task;
	bool need_more_memo;
	Stage stage;
	int task_size; // number of bytes
	Task* task;
} TaskPack;

// includes result and more info which is returned by process_task function
typedef struct ResultPack
{
	int result_size;
	Result* result;
} ResultPack;

// includes analysis and more info which is returned by process_result function
typedef struct AnalysisPack
{
	bool need_more_memo;	
	bool send_to_center; // send analysis to center or not
	int analysis_size;
	Analysis* analysis;
} AnalysisPack;

// structure used by center and sent to generate_task to use updates from receiver
typedef struct node {
	Analysis* analysis;
	struct node * next;
} AnalysisNode;

/*[[[cog
if new_problem:
	for line in lines:
		if line.startswith('generate_task'):
			filepath = line.split()[-1]
			cog.outl("// comes from %s" % filepath)
			codefile = open(filepath, "r")
			code = codefile.read()
			cog.outl("%s" % code)
			break
else:
	cog.out("""
// DEFAULT: sample generate_task function which runs for 100 rounds and 
// generates task containing the time that should be elapsed for processing it and some dummy data
TaskPack* generate_task(int complexity, int memo_size, int* memo, AnalysisNode* firstAnalysisNode)
{
	// takes some time to generate a task
	double t1 = MPI_Wtime();
	double t2 = t1;
	while(t2 - t1 < 1.0 / 100.0)
		t2 = MPI_Wtime();

	// here memo 0 indicates the number of the task
	memo[0]++; 
	TaskPack* taskpack = (TaskPack*) malloc(sizeof(TaskPack));

	// if no more task then announce it, else give the task
	// in this example of generate_task function we assume we have 50 tasks
	if(memo[0] > 50)
		taskpack -> no_more_task = true;
	else
	{
		taskpack -> no_more_task = false;
		// just add dummy data to task to make it bigger and see how it affects the performance
		int num_of_dummy_ints = 10000;
		taskpack -> task_size = sizeof(Task) + num_of_dummy_ints * sizeof(int);
		taskpack -> task = (Task*) malloc(taskpack -> task_size);
		taskpack -> task -> estimated_time = 6 * complexity;
		int ii = 0;
		for(ii = 0; ii < num_of_dummy_ints; ii++)
			taskpack -> task -> dummy_data[ii] = 10 * ii;
	}

	// make a request in the middle round just to test how more memory request works
	if(memo[0] == 25)
		taskpack -> need_more_memo = true;
	else
		taskpack -> need_more_memo = false;

	// fix the stage
	if(memo[0] < 10)
		taskpack -> stage = INITIAL;
	else if(memo[0] < 40)
		taskpack -> stage = MIDDLE;
	else
		taskpack -> stage = FINAL;

	// to make testing deterministic
	taskpack -> task -> task_num = memo[0];

	return taskpack;
}
	""")
]]]*/
//[[[end]]]
// select a server with estimated least task remained to do the next task
int select_server_for_task(int world_size, int* given_tasks, int* receiver_updates, int* emergency_updates)
{
	int current_server = 0, least_remained = 0; // server with least tasks remained
	int ii = 1;
	for(ii = 1; ii < world_size - 1; ii++)
		if(current_server == 0 || least_remained > given_tasks[ii] - MAX(receiver_updates[ii], emergency_updates[ii]))
		{
			current_server = ii;
			least_remained = given_tasks[ii] - MAX(receiver_updates[ii], emergency_updates[ii]);
		}
	return current_server;
}

// sets the complexity: an integer between 1 and 10
int set_task_complexity(Stage stage, int least_remained, int prev_complexity)
{
	// complexity of next task is related to the least remained task among all processes
	int comp = (int)(log(least_remained + 1)) + 1;
	/* since we want workers to finish at approximately the same time
	   if it was final stages we don't want to assign big tasks even if we had time to do so */
	if(stage == FINAL)
		comp = MIN(comp, 2);
	// to prevent the complexity from changing drastically
	// comp = (comp + prev_complexity) / 2;
	return MIN(comp, 10);
}

// the center
void center(int world_size, int world_rank)
{
	bool task_left = true;
	Stage stage = INITIAL;
	int complexity = set_task_complexity(stage, 0, 1);
	// keeping the number of task given and done for computing complexity and task assignment
	int* given_tasks = (int*) calloc(world_size, sizeof(int));
	int* receiver_updates = (int*) calloc(world_size, sizeof(int));		
	int* emergency_updates = (int*) calloc(world_size, sizeof(int));
	// memory used by generate_task function
	int memo_size = initial_center_memo_size;
	int* memo = (int*) calloc(memo_size, sizeof(int));
	// linked memory for analysis received from receiver
	AnalysisNode* firstAnalysisNode = NULL;
	while(task_left)
	{
		if(send_analysis_to_center && stage != INITIAL) // check for analysis from receiver
		{
			int flag = 0;
			MPI_Status status;
			MPI_Iprobe(world_size - 1, 5, MPI_COMM_WORLD, &flag, &status);
			if(flag) // analysis from receiver received
			{
				printf("from process %d: received analysis from receiver\n", world_rank);
				int analysis_size;
				MPI_Get_count(&status, MPI_BYTE, &analysis_size);
				Analysis* analysis = (Analysis*) malloc(analysis_size);
				MPI_Recv(analysis, analysis_size, MPI_BYTE, world_size - 1, 5, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				AnalysisNode* newAnalysisNode = (AnalysisNode*) malloc(sizeof(AnalysisNode));
				newAnalysisNode -> analysis = analysis;
				newAnalysisNode -> next = firstAnalysisNode;
				firstAnalysisNode = newAnalysisNode;
			}
		}
		//generate the task
		TaskPack* taskpack = generate_task(complexity, memo_size, memo, firstAnalysisNode);
		if(taskpack -> no_more_task) // no more task is remained
		{
			// let everybody know center is finished
			int ii = 1, signal = -1;
			for(ii = 1; ii < world_size; ii++)
				MPI_Send(&signal, 1, MPI_INT, ii, 4, MPI_COMM_WORLD);				
			task_left = false;
			free(taskpack);	
			continue;
		}
		stage = taskpack -> stage; // update on stage
		if(taskpack -> need_more_memo) //  more memory requested
		{
			int* more_memo = (int*) realloc(memo, 2 * memo_size * sizeof(int));
			if (more_memo != NULL)
			{
				memo_size *= 2;	
				memo = more_memo;
			}
			else
				printf ("Error! Could not allocate more memory :(\n");
		}
		int task_size = taskpack -> task_size;
		Task* task = taskpack -> task;
		// assign a server to generated task
		int assigned_server = -1;
		int flag = 0;
		MPI_Status status;
		if(scheduling_algorithm == PULL || (scheduling_algorithm == COMBTHENPULL && stage == FINAL)) // wait until emergency request is received
		{
			MPI_Probe(MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &status);
			flag = 1;
		}
		else if(scheduling_algorithm != PUSH) // check if emergency request has arrived
			MPI_Iprobe(MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &flag, &status);
		// if emergency task request is received: give next task to sender
		if(flag)
		{
			int msg_source = status.MPI_SOURCE;
			int sendtime;
			MPI_Recv(&sendtime, 1, MPI_INT, msg_source, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			emergency_updates[msg_source] = given_tasks[msg_source]; // server has done all tasks it is assigned to
			assigned_server = msg_source; // we surely want this server to get the next task
			printf("from process %d: received emergency task request from server %d\n", world_rank, msg_source);			
		}
		// else: see if there is any updates on finished tasks and give the task to server with estimated least remained tasks
		else
		{
			MPI_Iprobe(world_size - 1, 3, MPI_COMM_WORLD, &flag, &status);
			if(flag) // update from receiver received
			{
				MPI_Recv(receiver_updates, world_size, MPI_INT, world_size - 1, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				printf("from process %d: received receiver updates\n", world_rank);				
			}
			assigned_server = select_server_for_task(world_size, given_tasks, receiver_updates, emergency_updates);
		}
		// send the task to the assigned server
		MPI_Send(task, task_size, MPI_BYTE, assigned_server, 0, MPI_COMM_WORLD);
		printf("from process %d: task %d with complexity %d assigned to server %d\n", world_rank, memo[0], complexity, assigned_server);
		/* least tasks remained among all servers
		   computed before updating given_tasks[assigned_server] */
		int least_remained = given_tasks[assigned_server] - MAX(receiver_updates[assigned_server], emergency_updates[assigned_server]);
		complexity = set_task_complexity(stage, least_remained, complexity);		
		given_tasks[assigned_server]++;
		if(task_size > 0)
			free(task);		
		free(taskpack);
	}
	free(given_tasks);
	free(receiver_updates);
	free(emergency_updates);
	free(memo);
}

/*[[[cog
if new_problem:
	for line in lines:
		if line.startswith('process_task'):
			filepath = line.split()[-1]
			cog.outl("// comes from %s" % filepath)
			codefile = open(filepath, "r")
			code = codefile.read()
			cog.outl("%s" % code)
			break
else:
	cog.out("""
// DEFAULT: sample process_task function which checks validity of task's dummy data, 
// elapses the estimated_time with some randomness and creates another dummy data
ResultPack* process_task(int task_size, Task* task)
{
	ResultPack* resultpack = (ResultPack*) malloc(sizeof(ResultPack));	

	// taks some time with some randomness to process the task
	double t1 = MPI_Wtime();
	double t2 = t1;
	srand(task -> task_num);
	int randomness = (rand() % task -> estimated_time ) - task -> estimated_time / 2;
	while(t2 - t1 < (double)(task -> estimated_time + randomness) / 100.0)
		t2 = MPI_Wtime();

	int num_of_dummy_ints = 10000;
	resultpack -> result_size = sizeof(Result) + (num_of_dummy_ints / 10) * sizeof(int);
	resultpack -> result = (Result*) malloc(resultpack -> result_size);
	resultpack -> result -> invalid_task = false;

	// check if the dummy data is acutally received correctly	
	int ii = 0;
	for(ii = 0; ii < num_of_dummy_ints; ii++)
		if(task -> dummy_data[ii] != 10 * ii)
		{
			printf("ERROR: data sent to server is not correct");
			resultpack -> result -> invalid_task = true;
			break;
		}

	// add dummy data to the result
	for(ii = 0; ii < num_of_dummy_ints / 10; ii++)
		resultpack -> result -> dummy_data[ii] = 10 * ii;

	return resultpack;
}
	""")
]]]*/
//[[[end]]]
// the server
void server(int world_size, int world_rank)
{
	bool task_left = true;
	while(task_left)
	{
		int flag = 0;
		MPI_Status status;
		// if scheduling is push-based, no need for emergency request
		if(scheduling_algorithm != PUSH)
		{
			MPI_Iprobe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
			if(!flag) // no message or task is left
			{
				int now = (int) time(NULL); 
				// send emergency task request to center including the current time
				MPI_Send(&now, 1, MPI_INT, 0, 2, MPI_COMM_WORLD);
				// wait until new task comes
				MPI_Probe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			}
		}
		else
			MPI_Probe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		int msg_tag = status.MPI_TAG;
		// if no other task is going to be assigned
		if(msg_tag == 4)
		{
			//let the receiver know that the process is finished
			int signal = -1;
			MPI_Send(&signal, 1, MPI_INT, world_size - 1, 4, MPI_COMM_WORLD);			
			task_left = false;
			continue;
		}
		// regular task given
		else if(msg_tag == 0)
		{
			int task_size;
			MPI_Get_count(&status, MPI_BYTE, &task_size);
			Task* task = (Task*) malloc(task_size);
			MPI_Recv(task, task_size, MPI_BYTE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			printf("server %d received task\n", world_rank);
			ResultPack* resultpack = process_task(task_size, task);
			int result_size = resultpack -> result_size;
			Result* result = resultpack -> result;
			// send result of the task to receiver
			MPI_Send(result, result_size, MPI_BYTE, world_size - 1, 1, MPI_COMM_WORLD);
			if(task_size > 0)
				free(task);
			if(result_size > 0)
				free(result);
			free(resultpack);
		}
	}
}

/*[[[cog
if new_problem:
	for line in lines:
		if line.startswith('process_result'):
			filepath = line.split()[-1]
			cog.outl("// comes from %s" % filepath)
			codefile = open(filepath, "r")
			code = codefile.read()
			cog.outl("%s" % code)
			break
else:
	cog.out("""
// DEFAULT: sample process_result function which sends a meaningless analysis without the need for sending it to center or allocating more memo
AnalysisPack* process_result(int result_size, Result* result, int memo_size, int* memo)
{
	AnalysisPack* analysispack = (AnalysisPack*) malloc(sizeof(AnalysisPack));
	analysispack -> need_more_memo = false;
	analysispack -> send_to_center = true;
	analysispack -> analysis_size = sizeof(Analysis);
	analysispack -> analysis = (Analysis*) malloc(analysispack -> analysis_size);
	analysispack -> analysis -> means_nothing = true;
	return analysispack;
}
	""")
]]]*/
//[[[end]]]
// the receiver
void receiver(int world_size, int world_rank)
{
	bool task_left = true;
	// memo is the space that process_result can save details of previous processes to use them in future
	int memo_size = initial_receiver_memo_size;
	int* memo = (int*) calloc(memo_size, sizeof(int));
	int* tasks_finished_by_servers = (int*) calloc(world_size, sizeof(int));
	int total_tasks_finished = 0;
	int number_of_tasks_to_update_center = world_size * period_adjuster_to_update_center;
	int number_of_finished_processes = 0;
	while(task_left)
	{
		MPI_Status status;
		MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		int msg_source = status.MPI_SOURCE;
		int msg_tag = status.MPI_TAG;
		// means one process is finished
		if(msg_tag == 4)
		{
			int signal;
			MPI_Recv(&signal, 1, MPI_INT, msg_source, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);			
			number_of_finished_processes++;
			printf("from process %d: process %d is finished\n", world_rank, msg_source);
			// if all other processes were finished receiver is also finished
			if(number_of_finished_processes == world_size - 1)
			{
				printf("from process %d: all processes are done\n", world_rank);
				task_left = false;
			}
			continue;			
		}
		// regular result given
		else if(msg_tag == 1)
		{
			total_tasks_finished++;
			tasks_finished_by_servers[msg_source]++;
			// update center every once in a while if scheduling algorithm is not PULL
			if(scheduling_algorithm != PULL && total_tasks_finished % number_of_tasks_to_update_center == 0 && total_tasks_finished > 0)
				MPI_Send(tasks_finished_by_servers, world_size, MPI_INT, 0, 3, MPI_COMM_WORLD);
			int result_size;
			MPI_Get_count(&status, MPI_BYTE, &result_size);
			Result* result = (Result*) malloc(result_size);			
			MPI_Recv(result, result_size, MPI_BYTE, msg_source, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			AnalysisPack* analysispack = process_result(result_size, result, memo_size, memo);
			int analysis_size = analysispack -> analysis_size;
			Analysis* analysis = analysispack -> analysis;
			if(analysispack -> need_more_memo) // more memory requested
			{
				int* more_memo = (int*) realloc(memo, 2 * memo_size * sizeof(int));
				if (more_memo != NULL)
				{
					memo_size *= 2;					
					memo = more_memo;
				}
				else
					printf ("Error! Could not allocate more memory :(\n");
			}
			// send analysis to center if allowed and needed
			if(send_analysis_to_center && analysispack -> send_to_center)
				MPI_Send(analysis, analysis_size, MPI_BYTE, 0, 5, MPI_COMM_WORLD);
			if(result_size > 0)
				free(result);
			if(analysis_size > 0)
				free(analysis);
			free(analysispack);
		}
	}
	free(memo);
	free(tasks_finished_by_servers);
}

int main(int argc, char** argv)
{
	// Initialize the MPI environment
	MPI_Init(&argc, &argv);

	// Get the number of processes
	int world_size;
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);

	// Get the rank of the process
	int world_rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

	if(world_rank == 0)
		center(world_size, world_rank);

	else if(world_rank == world_size - 1)
		receiver(world_size, world_rank);

	else
		server(world_size, world_rank);

	// Finalize the MPI environment.
	MPI_Finalize();

	return 0;
}
