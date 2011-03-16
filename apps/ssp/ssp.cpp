#include "Pregel.h"
#include <set>
#include <iostream>
#include <stdio.h>
using namespace std;
using namespace Pregel;

typedef int ValueT;
typedef int EdgeT;
typedef int MessageT;

const int MAX_DIST=65536;
const int MOD_DIST=10;
const int N=6144;
const int N_Parts=96;
const int N_Edges=5;

class SSPCombiner:public BaseCombiner<MessageT>{
public:
	inline void combine(int & old, const int & new_msg){
		if(old>new_msg)
			old=new_msg;
	}
};

class SSPVertex:public BaseVertex<ValueT, EdgeT, MessageT, DefaultHash, SSPCombiner>{ // valueT=set<VertexID>, edgeT=void, messageT=set<VertexID>
	typedef BaseVertex<ValueT, EdgeT, MessageT, DefaultHash, SSPCombiner> BaseVertexType;
public:
	SSPVertex(VertexID id, const int & s):BaseVertexType(id,s){};
	SSPVertex(VertexID id):BaseVertexType(id){};
	SSPVertex(){};

	void compute(const MessageContainer & messages){
		WorkerLog("Vertex %d in worker %d\n",id(),get_worker_id());
		int min=value();
		for(MessageIter it=messages.begin(); it!=messages.end(); ++it){
			if(min>*it)
				min=*it;
		}
		if(min<value() || step_num()==0){
			value()=min;
			for(EdgeIter eit=edges().begin(); eit!=edges().end(); ++eit){
				send_message(eit->target, min+eit->value);
			}
		}
		else{
			vote_for_halt();
		}
	}
};


class SSPGraphLoader:public BaseGraphLoader<SSPVertex>{
public:
	void load_graph(const std::string & input_file){
		int me=get_worker_id();
		int np=get_num_workers();
		int nodes_per_worker=N/np;
		for(int i=0;i<nodes_per_worker;i++){
			int nid=me*nodes_per_worker+i;
			srand(nid);
			int dist=65536;
			if(nid==0)
				dist=0;
			add_vertex(nid, dist);
			for(int j=0;j<N_Edges;j++)
				add_edge(nid, (nid+rand())%N, rand()%MOD_DIST);
		}
	}
};

class SSPGraphDumper:public BaseGraphDumper<SSPVertex>{
public:
	void dump_partition(const string & output_file, const std::vector<SSPVertex> & vertexes){
		if(id()==0){
			for(int i=0;i<vertexes.size() && i<10; i++){
				const SSPVertex & v=vertexes[i];
				cout<<"vertex "<<v.id()<<": "<<v.value()<<endl;
			}
		}
	}
};


int main(int argc, char ** argv){
	init_pregel(argc,argv);
//	int i;
//	cin>>i;
	Worker<SSPVertex, SSPGraphLoader, SSPGraphDumper> worker;
	WorkerParams params;
	params.num_partitions=N_Parts;
	params.input_file="input";
	params.output_file="output";
	worker.run(params);
	return 0;
}
