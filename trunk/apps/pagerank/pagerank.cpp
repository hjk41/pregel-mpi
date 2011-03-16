#include "Pregel.h"
#include <set>
#include <iostream>
#include <stdio.h>
using namespace std;
using namespace Pregel;

typedef double ValueT;
typedef void EdgeT;
typedef double MessageT;

const int N=6144;
const int N_Parts=96;
const int N_Edges=20;

class PRCombiner:public BaseCombiner<MessageT>{
public:
	inline void combine(double & old, const double & new_msg){
		old+=new_msg;
	}
};

class PRVertex:public BaseVertex<ValueT, EdgeT, MessageT, DefaultHash, PRCombiner>{ // valueT=set<VertexID>, edgeT=void, messageT=set<VertexID>
	typedef BaseVertex<ValueT, EdgeT, MessageT, DefaultHash, PRCombiner> BaseVertexType;
public:
	PRVertex(VertexID id, const double & s):BaseVertexType(id,s){};
	PRVertex(VertexID id):BaseVertexType(id){};
	PRVertex(){};

	void compute(const MessageContainer & messages){
		WorkerLog("Vertex %d in worker %d\n",id(),get_worker_id());
		double sum=0;
		for(MessageIter it=messages.begin(); it!=messages.end(); ++it){
			sum+=*it;
		}
		if(step_num()>=30){
			vote_for_halt();
			return;
		}
		else{
			value()=0.15/N+0.85*sum;
			int n_edges=edges().size();
			double to_send=value()/n_edges;
			for(EdgeIter eit=edges().begin(); eit!=edges().end(); ++eit){
				send_message(eit->target, to_send);
			}
		}
	}
};

class PRGraphLoader:public BaseGraphLoader<PRVertex>{
public:
	void load_graph(const std::string & input_file){
		int me=get_worker_id();
		int np=get_num_workers();
		int nodes_per_worker=N/np;
		for(int i=0;i<nodes_per_worker;i++){
			int nid=me*nodes_per_worker+i;
			srand(nid);
			add_vertex(nid, (double)(rand()%1000)/1000);
			for(int j=0;j<N_Edges;j++){
				add_edge(nid, (nid+rand())%N);
			}
		}
	}
};

class PRGraphDumper:public BaseGraphDumper<PRVertex>{
public:
	void dump_partition(const string & output_file, const std::vector<PRVertex> & vertexes){
		if(id()==0){
			for(int i=0;i<vertexes.size() && i<10; i++){
				const PRVertex & v=vertexes[i];
				cout<<"vertex "<<v.id()<<": "<<v.value()<<endl;
			}
		}
	}
};


int main(int argc, char ** argv){
	init_pregel(argc,argv);
//	int i;
//	cin>>i;
	Worker<PRVertex, PRGraphLoader, PRGraphDumper> worker;
	WorkerParams params;
	params.num_partitions=N_Parts;
	params.input_file="input";
	params.output_file="output";
	worker.run(params);
	return 0;
}
