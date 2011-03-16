#ifndef GRAPHBUFFER_H
#define GRAPHBUFFER_H

#include <vector>
#include <algorithm>
#include "BaseVertex.h"
#include "Partition.h"
#include "Defines.h"
#include "Communication.h"
#include "Alltoall.h"
#include "Marshall.h"

namespace Pregel{

template<class VertexT>
class GraphBuffer{
	typedef Partition<VertexT> PartitionT;
	typedef std::vector<PartitionT> PartitionContainerT;
	typedef typename PartitionContainerT::iterator PartitionIterT;
	typedef typename VertexT::ValueType ValueType;
	typedef typename VertexT::EdgeType EdgeType;
	typedef typename VertexT::HashType HashType;
public:
	inline void add_vertex(VertexID id, const ValueType & v){
		WorkerLog("GraphBuffer::add_vertex\n");
		int part_id=hash(id,num_partitions);
		WorkerLog("part_id: %d\n",part_id);
		PartitionT new_p(part_id);
		PartitionIterT it=lower_bound(all_partitions.begin(), all_partitions.end(), new_p);
		if(it==all_partitions.end() || *it!=new_p){	// partition does not exist, insert one
			it=all_partitions.insert(it,new_p);
		}
		it->add_vertex(id,v);
	};
	inline void add_edge(VertexID src, const BaseEdge<EdgeType> & e){
		int part_id=hash(src,num_partitions);
		PartitionIterT it=find_sorted_by_id(all_partitions, part_id);
		assert(it->id()==part_id);
		it->add_edge(src,e);
	};
	void sync_graph(){
		WorkerLog("synchronizing graph\n");
		int me=get_worker_id();
		int np=get_num_workers();
		// put the partitions to sent together
		std::vector<std::vector<PartitionT *> > to_exchange(np);
		for(PartitionIterT it=all_partitions.begin(); it!=all_partitions.end(); ++it){
			int wid=(*partition_worker)[it->id()];
			if(wid==me){
			// if my partition, simply assign
				_my_partitions.push_back(*it);
			}
			else{
				to_exchange[wid].push_back(&(*it));
			}
		}
		// all to all exchange
		all_to_all(to_exchange);
		// gather all my partitions
		WorkerLog("Worker %d: before exchange: _my_partitions.size()=%d\n", get_worker_id(), _my_partitions.size());
		for(int i=0;i<np;i++){
			if(i==me)
				continue;
			std::vector<PartitionT *> & parts=to_exchange[i];
			for(int j=0;j<parts.size();j++){
				PartitionT & p=*(parts[j]);
				PartitionIterT it=lower_bound(_my_partitions.begin(), 
					_my_partitions.end(), p);
				if(it!=_my_partitions.end() && *it==p){
					it->merge_with(p);
				}
				else{
					_my_partitions.insert(it, p);
				}
			}
		}
		WorkerLog("Worker %d: after exchange: _my_partitions.size()=%d\n", get_worker_id(), _my_partitions.size());
		all_partitions.clear();
	};

	inline PartitionContainerT & my_partitions(){return _my_partitions;};
	inline void set_pw(std::vector<WorkerID> * pw){partition_worker=pw;}
	inline void set_num_partitions(int n){num_partitions=n;}
private:
	HashType hash;
	int num_partitions;
	PartitionContainerT _my_partitions;
	PartitionContainerT all_partitions;
	std::vector<WorkerID> * partition_worker;
};

};

#endif
