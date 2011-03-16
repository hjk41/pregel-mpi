#ifndef BASEVERTEX_H
#define BASEVERTEX_H

#include "Defines.h"
#include "Communication.h"
#include "BaseCombiner.h"
#include "MessageBuffer.h"
#include "Marshall.h"
#include "BaseAggregator.h"

namespace Pregel{

extern int global_step_num;

//=======================
// Hash 
// hashes vertexID to partition ID
class DefaultHash{
public:
	inline PartitionID operator()(const VertexID id, int num_partitions){
		return id%num_partitions;
	}
};

//=======================
// Edge class
//
template<class ValueT>
struct BaseEdge{
	BaseEdge(VertexID id, const ValueT & v):target(id),value(v){}
	BaseEdge(){};
	VertexID target;
	ValueT value;

	inline bool operator < (const BaseEdge<ValueT> & rhs){return target<rhs.target;}
	inline bool operator == (const BaseEdge<ValueT> & rhs){return target==rhs.target;}
	inline bool operator != (const BaseEdge<ValueT> & rhs){return target!=rhs.target;}
};
template<class ValueT>
Marshall & operator<<(Marshall & m, const BaseEdge<ValueT> & e){
	m<<e.target;
	m<<e.value;
	return m;
}
template<class ValueT>
Unmarshall & operator>>(Unmarshall & m, BaseEdge<ValueT> & e){
	m>>e.target;
	m>>e.value;
	return m;
}
// partial specification
template<>
struct BaseEdge<void>{
	BaseEdge(VertexID id):target(id){}
	BaseEdge(){};
	VertexID target;

	inline bool operator < (const BaseEdge<void> & rhs){return target<rhs.target;}
	inline bool operator == (const BaseEdge<void> & rhs){return target==rhs.target;}
	inline bool operator != (const BaseEdge<void> & rhs){return target!=rhs.target;}
};
template<>
Marshall & operator<<(Marshall & m, const BaseEdge<void> & e){
	m<<e.target;
	return m;
}
template<>
Unmarshall & operator>>(Unmarshall & m, BaseEdge<void> & e){
	m>>e.target;
	return m;
}

//=======================
// Vertex class
//
template<class ValueT, class EdgeValueT, class MessageT, 
	class HashT=DefaultHash, 
	class CombinerT=BaseCombiner<MessageT>, 
	class AggregatorT=BaseAggregator<void> >
class BaseVertex{
public:
	typedef MessageT MessageType;
	typedef ValueT ValueType;
	typedef EdgeValueT EdgeType;
	typedef HashT HashType;
	typedef CombinerT CombinerType;
	typedef AggregatorT AggregatorType;
	typedef BaseVertex<ValueType, EdgeType, MessageType, HashType, CombinerType,AggregatorT> BaseVertexType;

	typedef std::vector<MessageType> MessageContainer;
	typedef typename MessageContainer::const_iterator MessageIter;
	typedef std::vector<BaseEdge<EdgeType> > EdgeContainer;
	typedef typename EdgeContainer::iterator  EdgeIter;

public:
	friend Marshall & operator<<(Marshall & m, const BaseVertexType & v){
		m<<v._id;
		m<<v._value;
		m<<v._edges;
		return m;
	}
	friend Unmarshall & operator>>(Unmarshall & m, BaseVertexType & v){
		m>>v._id;
		m>>v._value;
		m>>v._edges;
		return m;
	}

	inline void compute(const MessageContainer & messages);
	inline ValueType & value(){return _value;}
	inline const ValueType & value()const{return _value;}
	inline const VertexID & id()const{return _id;}
	inline VertexID & id(){return _id;}
	inline EdgeContainer & edges(){return _edges;}

	void send_message(VertexID id, const MessageType & msg){
		((MessageBuffer<MessageType,HashType,CombinerType>*)get_message_buffer())->add_message(id,msg);
	}
	int step_num(){
		return global_step_num;
	}
	AggregatorT & aggregator(){
		return *((AggregatorT*)_get_aggregator());
	}
	const AggregatorT & aggregator()const{
		return *((AggregatorT*)_get_aggregator());
	}
public:
	BaseVertex(){};
	BaseVertex(VertexID i):_id(i){};
	BaseVertex(VertexID i, const ValueType & v):_id(i),_value(v){};
	BaseVertex(const BaseVertex & rhs){*this=rhs;};
	BaseVertex & operator=(const BaseVertex & r){
		BaseVertex & rhs=const_cast<BaseVertex &>(r);
		_id=rhs._id; 
		_value=rhs._value; 
		_edges.swap(rhs._edges); 
		return *this;
	}
	inline bool operator < (const BaseVertexType & rhs){return _id<rhs._id;}
	inline bool operator == (const BaseVertexType & rhs){return _id==rhs._id;}
	inline bool operator != (const BaseVertexType & rhs){return _id!=rhs._id;}
	inline void add_edge(const BaseEdge<EdgeValueT> & new_edge){
		insert_sorted(_edges, new_edge);
	}
private:
	VertexID _id;
	ValueT _value;
	EdgeContainer _edges;	// ordered vector
};

};	// namespace Pregel
#endif	// ifdef PREGEL_H
