#ifndef MESSAGEBUFFER_H
#define MESSAGEBUFFER_H

#include <vector>
#include <algorithm>
#include "Defines.h"
#include "Communication.h"
#include "Marshall.h"
#include "Alltoall.h"

#include <ext/hash_map>
#define my_hash_map __gnu_cxx::hash_map
//#include <map>
//#define my_hash_map std::map

namespace Pregel{

template<class MessageT>
struct IDMessage{
	VertexID id;
	std::vector<MessageT> * messages;

	IDMessage():id(0),messages(NULL){};
	IDMessage(VertexID i, std::vector<MessageT> * m):id(i),messages(m){};
	friend Marshall & operator<<(Marshall & m, const IDMessage<MessageT> & idm){
		m<<idm.id;
		m<<(*idm.messages);
		return m;
	}
	friend Unmarshall & operator>>(Unmarshall &m, IDMessage<MessageT> & idm){
		m>>idm.id;
		idm.messages=new std::vector<MessageT>;
		m>>*(idm.messages);
		return m;
	}
};

//--------------------------
// work around the lack for template function partial specialization
template<class CombinerT>
class DummyCombiner{
public:
	inline void _add_message(my_hash_map<VertexID, std::vector<typename CombinerT::MessageType> > & message_map, 
		VertexID id, const typename CombinerT::MessageType & msg){
		std::vector<typename CombinerT::MessageType> & messages=message_map[id];
		WorkerLog("_add_message: id=%d, messages.size=%d\n",id,messages.size());
		if(messages.size()==0){
			messages.push_back(msg);
		}
		else{
			combiner.combine( messages[0], msg );
		}
	}
private:
	CombinerT combiner;
};

template<class MessageType>
class DummyCombiner<BaseCombiner<MessageType> >{
public:
	inline void _add_message(my_hash_map<VertexID, std::vector<MessageType> > & message_map, 
		VertexID id, const MessageType & msg){
		message_map[id].push_back(msg);
	}
};

//-----------------------------
// the actual message buffer

template<class MessageT, class HashT, class CombinerT>
class MessageBuffer{
	typedef std::vector<MessageT> MessageContainerT;
	typedef my_hash_map<VertexID, MessageContainerT> HashMap;
	typedef typename HashMap::iterator HashMapIter;
public:
	void add_message(VertexID id, const MessageT & msg){
		combiner._add_message(out_messages, id, msg);
		WorkerLog("worker %d: add_message(dst=%d), out_messages.size()=%d\n", get_worker_id(), id, out_messages.size());
	}

	MessageContainerT & get_messages(VertexID id){
		return in_messages[id];
	}

	void sync_messages(){
		int np=get_num_workers();
		int me=get_worker_id();
		// clear input messages
		in_messages.clear();
		WorkerLog("worker %d: before sync_messages, out_messages.size()=%d\n",me, out_messages.size());
		// get messages from local
		std::vector<std::vector<IDMessage<MessageT> > > to_exchange(np);
		for(HashMapIter it=out_messages.begin(); it!=out_messages.end(); ++it){
			int vid=it->first;
			int wid=vertex_worker(vid);
			WorkerLog("worker %d: message to vertex %d sent to worker %d\n", me, vid, wid);
			if(wid==me){
				// my own message, just add to in_messages
				in_messages[vid].swap(it->second);
			}
			else{
				to_exchange[wid].push_back(IDMessage<MessageT>(vid,&(it->second)));
			}
		}
		WorkerLog("worker %d before message exchange: in_messages.size()=%d\n", me, in_messages.size());
		// get messages from remote
		all_to_all(to_exchange);
		// gather all messags
		for(int i=0;i<np;i++){
			if(i==me)
				continue;
			std::vector<IDMessage<MessageT> > & messages=to_exchange[i];
			for(int j=0;j<to_exchange[i].size();j++){
				IDMessage<MessageT> & idm=messages[j];
				MessageContainerT & local_m=in_messages[idm.id];
				local_m.insert(local_m.end(), idm.messages->begin(), idm.messages->end());
			}
		}
		WorkerLog("worker %d after message exchange: in_messages.size()=%d\n", me, in_messages.size());
		out_messages.clear();
	}

	void set_pw(std::vector<WorkerID> * pw){partition_worker=pw;};
	void set_num_partitions(int n){num_partitions=n;}
	int vertex_worker(VertexID id){
		return (*partition_worker)[hash(id,num_partitions)];
	}
private:
	HashMap out_messages;
	HashMap in_messages;

	std::vector<WorkerID> * partition_worker;
	HashT hash;
	int num_partitions;
	DummyCombiner<CombinerT> combiner;
};

};

#endif
