// Not Default!!!
typedef struct Task 
{
	int estimated_time;
	// data inside is not important for the logic
	// it is just to check the performance of communication with big size tasks
	int dummy_data[];
} Task;
