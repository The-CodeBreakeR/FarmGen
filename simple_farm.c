/* TODO plans for the rest of the project / more detailed milestones:

   -change the return type of task-gen, and process functions to a better defined structure (containing the task itself and the additional/config info) [milestone 3]
   -change the data structure of the task itself to something better than an array of ints [milestone 3]
   -try to fetch out parameters from the center/workers functions and add them to configuration (separate policy from mechanism) [milestone 4]
   	* when worker sends emergency msg to center
	*
	*
   -python code to generate them all [milestone 2]
   -seems developing complicated examples is not the priority [milestone 5]
   	*having the task just indicate how much time should be elapsed (and maybe some dummy memory to see how the communication works for big data)
   -(not required) receiver could also send feedbacks from processed results to center [milestone 3]
   */

/* DONE targets:
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
4: finish (center -> all)
*/

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) > (y)) ? (y) : (x))

const int initial_center_memo_size = 100000;
const int initial_receiver_memo_size = 100000;
const int period_adjuster_to_update_center = 1;

int* generate_task(int complexity, int memo_size, int* memo)
{
	int* rt = (int*) malloc(sizeof(int) * 3);

	// takes some time to generate a task
	double t1 = MPI_Wtime();
	double t2 = t1;
	while(t2 - t1 < 1.0)
		t2 = MPI_Wtime();

	memo[0]++;
	if(memo[0] < 10)
		rt[0] = 0;
	else if(memo[0] > 90)
		rt[0] = 2;
	else if(memo[0] == 50)
		rt[0] = 3;
	else
		rt[0] = 1;
	if(memo[0] > 100)
		rt[1] = -1;
	else
	{
		rt[1] = 1;
		rt[2] = 6 * complexity;
	}
	return rt;
}

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
int set_task_complexity(int stage, int least_remained, int prev_complexity)
{
	int comp = (int)(log(least_remained + 1)) + 1;
	if(stage == 2)
		comp = MIN(comp, 2);
	return MIN(comp, 10);
}

void center(int world_size, int world_rank)
{
	bool task_left = true;
	// 0 means initial stages, 1 is middle stages, 2 is final stages
	int stage = 0;
	int* given_tasks = (int*) calloc(world_size, sizeof(int));
	int* receiver_updates = (int*) calloc(world_size, sizeof(int));		
	int* emergency_updates = (int*) calloc(world_size, sizeof(int));
	int complexity = set_task_complexity(stage, 0, 0);
	int memo_size = initial_center_memo_size;
	int* memo = (int*) calloc(memo_size, sizeof(int));
	while(task_left)
	{
		//generate the task
		int* return_val = generate_task(complexity, memo_size, memo);
		if(return_val[0] < 3) // update on stage
			stage = return_val[0];
		if(return_val[0] == 3) //  more memory requested
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
		if(return_val[1] == -1) // no more task is remained
		{
			int ii = 1;
			for(ii = 1; ii < world_size; ii++)
				MPI_Send(&(return_val[1]), 1, MPI_INT, ii, 4, MPI_COMM_WORLD);				
			task_left = false;
			free(return_val);			
			continue;
		}
		int task_size = return_val[1];
		int* task = &(return_val[2]);
		// assign a server to generated task
		int assigned_server = -1;				
		int flag = 0;
		MPI_Status status;
    		MPI_Iprobe(MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &flag, &status);
		if(flag) // emergency task request received
		{
			int msg_source = status.MPI_SOURCE;
			int sendtime;
			MPI_Recv(&sendtime, 1, MPI_INT, msg_source, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			emergency_updates[msg_source] = given_tasks[msg_source]; // server has done all tasks it is assigned to
			assigned_server = msg_source; // we surely want this server to get the next task
			printf("from process %d: received emergency task request from server %d\n", world_rank, msg_source);			
		}
		if(assigned_server == -1)
		{
    			MPI_Iprobe(world_size - 1, 3, MPI_COMM_WORLD, &flag, &status);
			if(flag) // update from receiver received
				MPI_Recv(receiver_updates, world_size, MPI_INT, world_size - 1, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			assigned_server = select_server_for_task(world_size, given_tasks, receiver_updates, emergency_updates);
			printf("receiver updates %d %d %d %d %d\n", receiver_updates[1], receiver_updates[2], receiver_updates[3], receiver_updates[4], receiver_updates[5]);
		}
		MPI_Send(task, task_size, MPI_INT, assigned_server, 0, MPI_COMM_WORLD);
		printf("from process %d: task %d with complexity %d assigned to server %d\n", world_rank, memo[0], complexity, assigned_server);
		// least tasks remained among all servers
		int least_remained = given_tasks[assigned_server] - MAX(receiver_updates[assigned_server], emergency_updates[assigned_server]);
		complexity = set_task_complexity(stage, least_remained, complexity);		
		given_tasks[assigned_server]++;
		free(return_val);
	}
	free(given_tasks);
	free(receiver_updates);
	free(emergency_updates);
	free(memo);
}

int* process_task(int task_size, int* task)
{
	int* rt = (int*) malloc(sizeof(int) * 2);

	// taks some time with some randomness to process the task
	double t1 = MPI_Wtime();
	double t2 = t1;
	srand(time(0));
	int randomness = (rand() % 7) - 3;
	while(t2 - t1 < task[0] + randomness)
		t2 = MPI_Wtime();

	rt[0] = 1;
	rt[1] = 23; // means the task is done
	return rt;
}

void server(int world_size, int world_rank)
{
	bool task_left = true;
	while(task_left)
	{
		int flag = 0;
		MPI_Status status;
    		MPI_Iprobe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
		if(!flag)
		{
			int now = (int) time(NULL); 
			// send emergency task request to center including the current time
			MPI_Send(&now, 1, MPI_INT, 0, 2, MPI_COMM_WORLD);
			// wait until new task comes
			MPI_Probe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		}
		int msg_tag = status.MPI_TAG;
		// if no other task is going to be assigned
		if(msg_tag == 4)
		{
			task_left = false;
			continue;
		}
		// regular task given in int datatype
		else if(msg_tag == 0)
		{
			int task_size;
			MPI_Get_count(&status, MPI_INT, &task_size);
			int* task = (int*) malloc(sizeof(int) * task_size);
			MPI_Recv(task, task_size, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			printf("server %d received task with volume %d\n", world_rank, task[0]);			
			int* return_val = process_task(task_size, task);
			int result_size = return_val[0];
			int* result = &(return_val[1]);
			MPI_Send(result, result_size, MPI_INT, world_size - 1, 1, MPI_COMM_WORLD);
			free(task);
			free(return_val);
		}
	}
}

int* process_result(int result_size, int* result, int memo_size, int* memo)
{
	int* rt = (int*) malloc(sizeof(int));
	rt[0] = 0;
	return rt;
}

void receiver(int world_size, int world_rank)
{
	bool task_left = true;
	// memo is the space that process_result can save details of previous processes to use them in future
	int memo_size = initial_receiver_memo_size;
	int* memo = (int*) calloc(memo_size, sizeof(int));
	int* tasks_finished_by_servers = (int*) calloc(world_size, sizeof(int));
	int total_tasks_finished = 0;
	int number_of_tasks_to_update_center = world_size * period_adjuster_to_update_center;
	while(task_left)
	{
		MPI_Status status;
    		MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		int msg_source = status.MPI_SOURCE;
		int msg_tag = status.MPI_TAG;
		// if no other task is going to be assigned
		if(msg_tag == 4)
		{
			task_left = false;
			continue;
		}
		// regular result given in int datatype
		else if(msg_tag == 1)
		{
			total_tasks_finished++;
			tasks_finished_by_servers[msg_source]++;
			if(total_tasks_finished % number_of_tasks_to_update_center == 0 && total_tasks_finished > 0) // update center
				MPI_Send(tasks_finished_by_servers, world_size, MPI_INT, 0, 3, MPI_COMM_WORLD);				
			int result_size;
			MPI_Get_count(&status, MPI_INT, &result_size);
			int* result = (int*) malloc(sizeof(int) * result_size);
			MPI_Recv(result, result_size, MPI_INT, msg_source, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			int* return_val = process_result(result_size, result, memo_size, memo);
			if(return_val[0] == 1) // more memory requested
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
			// TODO do something with the processed result, probably final analysis or sending msg to center
			free(result);
			free(return_val);
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
