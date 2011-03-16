#ifndef BASEGRAPHLOADER_H
#define BASEGRAPHLOADER_H

#include <string>
#include "BaseVertex.h"
#include "Defines.h"
#include "GraphBuffer.h"

namespace Pregel{


//======================
// class for edge insertion
template<class VertexT, class EdgeValueT>
class GLEdgeInserter{
public:
	void add_edge(VertexID src, VertexID end, const EdgeValueT & v){
		WorkerLog("adding edge %d -> %d with value\n",src,dst);
		_get_graph_buffer()->add_edge(src, BaseEdge<EdgeValueT>(end, v));
	}
	virtual GraphBuffer<VertexT> * _get_graph_buffer()=0;
};

template<class VertexT>
class GLEdgeInserter<VertexT, void>{
public:
	void add_edge(VertexID src, VertexID end){
		WorkerLog("adding edge %d -> %d\n",src,end);
		_get_graph_buffer()->add_edge(src, BaseEdge<void>(end));
	}
	virtual GraphBuffer<VertexT> * _get_graph_buffer()=0;
};

//=======================
// Graph loader
// loads vertex from file
//
template<class VertexT>
class BaseGraphLoader:public GLEdgeInserter<VertexT, typename VertexT::EdgeType>{
	typedef typename VertexT::ValueType ValueType;
	typedef typename VertexT::EdgeType EdgeType; 
public:
	virtual ~BaseGraphLoader(){};
	void load_graph(const std::string & file){};
	inline void set_id(WorkerID i){_id=i;}
	inline void set_buffer(GraphBuffer<VertexT> * b){graph_buffer=b;};
public:
	inline void add_vertex(VertexID id, const ValueType & v){
		WorkerLog("adding vertex %d\n",id);
		graph_buffer->add_vertex(id, v);
	}
	inline WorkerID id(){return _id;}
	// add_edge calls are inheritated from GLEdgeInserter
	// here we provide the graphbuffer
	virtual GraphBuffer<VertexT> * _get_graph_buffer(){return graph_buffer;}
private:
	WorkerID _id;
	GraphBuffer<VertexT> * graph_buffer; 
};

};

#endif
