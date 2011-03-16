#ifndef WORKER_H
#define WORKER_H

#include <vector>
#include <string>
#include "Defines.h"
#include "Communication.h"
#include "Partition.h"
#include "MessageBuffer.h"
#include "GraphBuffer.h"
#include "BaseAggregator.h"

namespace Pregel{

extern int global_step_num;

//------------------------
// worker parameters
struct WorkerParams{
	int num_partitions;
	std::string input_file;
	std::string output_file;
};

//=======================
// worker
//
template<class VertexT, class GraphLoaderT, class GraphDumperT>
class Worker{
	typedef Partition<VertexT> PartitionT;
	typedef std::vector<PartitionT> PartitionContainerT;
	typedef typename PartitionContainerT::iterator PartitionIterT;
	typedef typename VertexT::MessageType MessageT;
	typedef typename VertexT::HashType HashT;
	typedef typename VertexT::CombinerType CombinerT;
	typedef typename VertexT::AggregatorType AggregatorT;
public:
	// create a worker 
	Worker(){
		id=get_worker_id();
		num_workers=get_num_workers();
		set_message_buffer(&message_buffer);
		_set_aggregator(&_aggregator);
	}

	~Worker(){
		worker_finalize();
	}

	// run the worker
	void run(const WorkerParams & params){
		ResetTimer(WORKER_TIMER);
		// set parameters
		num_partitions=params.num_partitions;
		input_file=params.input_file;
		output_file=params.output_file;
		// init
		assign_partitions();	// assign partition to workers
		WorkerLog("worker init finished\n");
		StopTimer(WORKER_TIMER);
		PrintTimer("Init Time",WORKER_TIMER);
		ResetTimer(WORKER_TIMER);
		// load graph
		GraphBuffer<VertexT> graph_buffer;
		graph_buffer.set_num_partitions(num_partitions);
		graph_buffer.set_pw(&partition_worker);
		GraphLoaderT loader;
		loader.set_id(id);
		loader.set_buffer(&graph_buffer);
		loader.load_graph(input_file);
		graph_buffer.sync_graph();
		partitions.swap(graph_buffer.my_partitions());
		worker_barrier();
		WorkerLog("load graph finished\n");
		StopTimer(WORKER_TIMER);
		PrintTimer("Load Time",WORKER_TIMER);
		init_timers();	// reinit timer so we calculate only computing time
		ResetTimer(WORKER_TIMER);
		// do steps
		bool halt=false;
		int step_num=0;
		while(!halt){
			do_step(step_num++);
			halt=all_halt();
		}
		worker_barrier();
		StopTimer(WORKER_TIMER);
		PrintTimer("Communication Time", COMMUNICATION_TIMER);
		PrintTimer("Compute Time",WORKER_TIMER);
		ResetTimer(WORKER_TIMER);
		// dump graph
		GraphDumperT dumper;
		for(PartitionIterT it=partitions.begin(); it!=partitions.end(); ++it){
			dumper.set_id(it->id());
			dumper.dump_partition(output_file, it->vertexes());
		}
		StopTimer(WORKER_TIMER);
		PrintTimer("Dump Time",WORKER_TIMER);
	}
private:
	void assign_partitions(){
		partition_worker.resize(num_partitions);
		for(int i=0;i<num_partitions;i++){
			partition_worker[i]=i%num_workers;
		}
	}
	void do_step(int step_num){
		WorkerLog("worker %d doing step %d\n", id, step_num);
		global_step_num=step_num;
		halt_partition_count=0;
		_aggregator.reset();
		message_buffer.set_pw(&partition_worker);
		message_buffer.set_num_partitions(num_partitions);
		for(PartitionIterT it=partitions.begin(); it!=partitions.end(); ++it){
			WorkerLog("worker %d executing partition %d, vertexes.size=%d\n", id, it->id(), it->vertexes().size());
//			if(!it->halt())
			it->all_compute();
			if(it->halt())
				halt_partition_count++;
		}
		WorkerLog("halt_partition_count %d\n",halt_partition_count);
		// sync messages
		message_buffer.sync_messages();
		// aggregate
		_aggregator.all_aggregate();
	}
	bool all_halt(){
		int all_halt_count=0;
		WorkerLog("my_halt_count :%d\n",halt_partition_count);
		all_halt_count=all_sum(halt_partition_count);
		WorkerLog("all_halt_count :%d\n",all_halt_count);
		return all_halt_count==num_partitions;
	}
private:
	WorkerID id;
	int num_workers;
	int num_partitions;
	std::string input_file;
	std::string output_file;
	int halt_partition_count;

	std::vector<WorkerID> partition_worker;
	PartitionContainerT partitions;
	MessageBuffer<MessageT, HashT, CombinerT> message_buffer;
	AggregatorT _aggregator;
};


};	// namespace Pregel
#endif	// ifdef WORKER_H
