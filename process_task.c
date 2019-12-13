// Not Default!!!
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
