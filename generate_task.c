// Not Default!!!
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
		taskpack -> task -> estimated_time = 6;
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
