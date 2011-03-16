#ifndef PARTITION_H
#define PARTITION_H

#include <set>
#include <algorithm>
#include "MessageBuffer.h"
#include "Defines.h"
#include "Communication.h"

namespace Pregel{

class Marshall;
class Unmarshall;

//-----------------------------------
// assume each partition uses a thread
//-----------------------------------
template<class VertexT>
class Partition{
	typedef std::vector<VertexT> VertexContainer;	// sorted vector
	typedef typename VertexContainer::iterator VertexIter;
	typedef typename VertexT::ValueType ValueType;
	typedef typename VertexT::EdgeType EdgeType;
	typedef typename VertexT::MessageType MessageType;
	typedef typename VertexT::HashType HashType;
	typedef typename VertexT::CombinerType CombinerType;
public:
	friend Marshall & operator<<(Marshall & m, const Partition<VertexT> & p){
		m<<p._id;
		m<<p._vertexes;
		return m;
	}

	friend Unmarshall & operator>>(Unmarshall & m, Partition<VertexT> & p){
		m>>p._id;
		m>>p._vertexes;
		return m;
	}

	Partition(){}
	Partition(PartitionID i):_id(i){};
	Partition(const Partition & rhs){*this=rhs;}
	Partition & operator=(const Partition & r){
		Partition & rhs=const_cast<Partition &>(r);
		_vertexes.clear(); 
		_id=rhs._id; 
		_vertexes.swap(rhs._vertexes); 
		return *this;
	}
	void merge_with(Partition & rhs){
		// merge two partitions, assuming no intersection
		assert(_id==rhs._id);
		VertexContainer new_v(_vertexes.size()+rhs._vertexes.size());
		std::merge(_vertexes.begin(), _vertexes.end(), 
			rhs._vertexes.begin(), rhs._vertexes.end(), new_v.begin());
		_vertexes.swap(new_v);
	}
	inline bool operator<(const Partition<VertexT> & rhs){return _id<rhs._id;}
	inline bool operator==(const Partition<VertexT> & rhs){return _id==rhs._id;}
	inline bool operator!=(const Partition<VertexT> & rhs){return _id!=rhs._id;}
	inline PartitionID id(){return _id;}
	inline VertexContainer & vertexes(){return _vertexes;}
	inline const VertexContainer & vertexes() const{return _vertexes;}

	inline void add_vertex(VertexID id, const ValueType & value){
		VertexT new_vertex(id,value);
		insert_sorted(_vertexes, new_vertex);
	}
	inline void add_edge(VertexID start, const BaseEdge<EdgeType> & e){
		VertexIter it=find_sorted_by_id(_vertexes, start);
		assert(it->id()==start);	// there must already be a vertex
		it->add_edge(e);
	}
	//----------------------
	// called by worker
	inline bool halt(){
		return get_halt_count()==_vertexes.size();
	}
	void all_compute(){
		set_halt_count(0);
		for(VertexIter vit=_vertexes.begin(); vit!=_vertexes.end(); ++vit){
			vit->compute(((MessageBuffer<MessageType, HashType, CombinerType> *)get_message_buffer())
					->get_messages(vit->id()));
		}
	}
	inline void clear(){
		_vertexes.clear();
	}
private:
	PartitionID _id;
	VertexContainer _vertexes;
};

};

#endif
