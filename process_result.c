// Not Default!!!
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
