#ifndef BASE_AGGREGATOR_H
#define BASE_AGGREGATOR_H

#include "Defines.h"
#include "Communication.h"
#include <stdio.h>

namespace Pregel{

template<class T>
class BaseAggregator{
public:
	BaseAggregator(){};
	virtual void aggregate(const T & to_be_aggregated)=0;
	inline T & value(){return aggregated_data;};
	inline const T & value()const{return aggregated_data;};
	
	virtual void reset(){value()=T();}

	inline void all_aggregate(){
		WorkerLog("aggregator: id=%d, value=%d\n",get_worker_id(),value());
		int me=get_worker_id();
		int np=get_num_workers();
		// ok, simplest algorithm first
		// TODO: improve this algorithm
		if(me==0){
			for(int i=1;i<np;i++){
				T received=recv_data<T>(i);
				WorkerLog("aggregator received %d from %d\n",received,i);
				aggregate(received);
				WorkerLog("aggregator aggregates to %d\n",value());
			}
			for(int i=1;i<np;i++){
				send_data(value(),i);
			}
		}
		else{
			send_data(value(),0);
			value()=recv_data<T>(0);
		}
		WorkerLog("aggregated: id=%d, value=%d\n",get_worker_id(),value());
	}
private:
	T aggregated_data;
};

template<>
class BaseAggregator<void>{
public:
	inline void all_aggregate(){};
	virtual void reset(){};
};

};


#endif
