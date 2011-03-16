#ifndef ALLTOALL_H
#define ALLTOALL_H

#include <vector>
#include "Marshall.h"
#include "Defines.h"
#include "Communication.h"

namespace Pregel{

template<class T>
void all_to_all(std::vector<T> & to_exchange){
	StartTimer(COMMUNICATION_TIMER);
	// for each to_exchange[i]
	//   send out *to_exchange[i] to UE i
	//   and save received data in *to_exchange[i]
	int np=get_num_workers();
	int me=get_worker_id();
	for(int i=0;i<np;i++){
		int partner=(i-me+np)%np;
		if(me!=partner){
			if(me<partner){
				StartTimer(MARSHALL_TIMER);
				// send, and then receive
				Marshall m;
				m<<to_exchange[partner];
				StopTimer(MARSHALL_TIMER);
				StartTimer(TRANSFER_TIMER);
				send_marshall(m, partner);
				StopTimer(TRANSFER_TIMER);
				// recv
				StartTimer(TRANSFER_TIMER);
				Unmarshall um=recv_unmarshall(partner);
				StopTimer(TRANSFER_TIMER);
				StartTimer(MARSHALL_TIMER);
				um>>to_exchange[partner];
				StopTimer(MARSHALL_TIMER);
			}
			else{
				StartTimer(TRANSFER_TIMER);
				// receive
				Unmarshall um=recv_unmarshall(partner);
				StopTimer(TRANSFER_TIMER);
				StartTimer(MARSHALL_TIMER);
				T received;
				um>>received;
				// send
				Marshall m;
				m<<to_exchange[partner];
				StopTimer(MARSHALL_TIMER);
				StartTimer(TRANSFER_TIMER);
				send_marshall(m, partner);
				StopTimer(TRANSFER_TIMER);
				to_exchange[partner]=received;
			}
		}
	}
	StopTimer(COMMUNICATION_TIMER);
}

};

#endif
