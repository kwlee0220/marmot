package marmot.mapreduce;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum PlanMRExecutionMode {
	/**
	 * local하게 수행가능한 경우.
	 * 일반적으로 입력 파일의 크기가 미리 정해진 크기 보다 작은 경우에 활용된다.
	 */
	LOCALLY_EXECUTABLE,
	/**
	 * 조건부로 local 수행이 가능한 경우.
	 * 입력 파일의 크기가 일정크기 보다 크지만, 이 데이터를 활용하는 연산이 정렬(sort)과 같은 
	 * 고부하 연산이 없는 경우에 한에 local 수행이 가능한 경우.
	 */
	CONDITIONALLY_EXECUTABLE,
	/**
	 * Local 수행이 부적합한 경우.
	 * 입력 파일의 크기가 너무 커서 local하게 수행할 수 없거나, 수행시간이 길어져 MapReduce를
	 * 활용하는 것이 더 효과적인 경우.
	 */
	NON_LOCALLY_EXECUTABLE,
	NON_MAPREDUCE_EXECUTABLE,
}