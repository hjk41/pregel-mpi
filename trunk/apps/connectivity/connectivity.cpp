#include "Pregel.h"
#include <set>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
using namespace std;
using namespace Pregel;

template<class T>
inline void my_set_union(set<T> & old, const set<T> & new_msg){
	old.insert(new_msg.begin(), new_msg.end());
}

class ConnCombiner:public BaseCombiner<set<VertexID> >{
public:
	inline void combine(set<VertexID> & old, const set<VertexID> & new_msg){
		my_set_union(old,new_msg);
	}
};

class ConnAggregator:public BaseAggregator<int>{
public:
	ConnAggregator(){value()=0;};
	void aggregate(const int & in){
		value()+=in;
	}
};

class ConnVertex:public BaseVertex<set<VertexID>, void, set<VertexID>, DefaultHash, ConnCombiner, ConnAggregator>{ // valueT=set<VertexID>, edgeT=void, messageT=set<VertexID>
	typedef BaseVertex<set<VertexID>, void, set<VertexID>, DefaultHash, ConnCombiner, ConnAggregator> BaseVertexType;
public:
	ConnVertex(VertexID id, const set<VertexID> & s):BaseVertexType(id,s){};
	ConnVertex(VertexID id):BaseVertexType(id){};
	ConnVertex(){};

	void compute(const MessageContainer & messages){
		WorkerLog("Vertex %d in worker %d\n",id(),get_worker_id());
		int last_size=value().size();
		set<VertexID> diff;
		for(MessageIter it=messages.begin(); it!=messages.end(); ++it){
			for(set<VertexID>::iterator sit=it->begin();sit!=it->end();++sit){
				if(value().find(*sit)==value().end()){	// new reachable
					value().insert(*sit);
					diff.insert(*sit);
				}
			}
		}
		aggregator().aggregate(diff.size());
		if(step_num()==0)
			diff.insert(id());
		if(diff.size()!=0){
			for(EdgeIter eit=edges().begin(); eit!=edges().end(); ++eit){
				send_message(eit->target, diff);
			}
		}
		else{
			vote_for_halt();
		}
	}
};

int N_Nodes=1536;
int N_Parts=96;
int N_Edges=5;

class ConnGraphLoader:public BaseGraphLoader<ConnVertex>{
public:
	void load_graph(const std::string & input_file){
		int me=id();
		int np=get_num_workers();
		int nodes_per_worker=N_Nodes/np;
		for(int i=0;i<nodes_per_worker;i++){
			int node_id=me*nodes_per_worker+i;
			add_vertex(node_id, set<VertexID>(&node_id,&node_id+1));
			for(int j=0;j<N_Edges;j++)
//				add_edge(node_id, (node_id+rand())%N_Nodes);
				add_edge(node_id, (node_id+j+1)%N_Nodes);
		}
	}
};

class ConnGraphDumper:public BaseGraphDumper<ConnVertex>{
public:
	void dump_partition(const string & output_file, const std::vector<ConnVertex> & vertexes){
		if(id()==0){
			for(int i=0;i<vertexes.size() && i<10; i++){
				const ConnVertex & v=vertexes[i];
				if(i==0)
					printf("num_new_edges: %d\n",v.aggregator().value());
				printf("vertex %d: %d\n", v.id(), v.value().size());
			}
		}
	}
};

int main(int argc, char ** argv){
	init_pregel(argc,argv);
	srand(10);
	Worker<ConnVertex, ConnGraphLoader, ConnGraphDumper> worker;
	WorkerParams params;
	N_Nodes=atoi(argv[1]);
	N_Edges=atoi(argv[2]);
	N_Parts=atoi(argv[3]);
	cout<<"using "<<N_Nodes<<" nodes"<<endl;
	cout<<"using "<<N_Edges<<" edges/node"<<endl;
	cout<<"using "<<N_Parts<<" partitions"<<endl;
	params.num_partitions=N_Parts;
	params.input_file="input";
	params.output_file="output";
	worker.run(params);
	cout<<"finished"<<endl;
	return 0;
}
