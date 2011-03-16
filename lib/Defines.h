#ifndef DEFINES_H
#define DEFINES_H

#include <mpi.h>
#include <vector>
#include <algorithm>
#include <assert.h>
#include <stdio.h>
#include "Time.h"

#define WorkerLog(...) //printf(__VA_ARGS__)
#define StartTimer(i) start_timer((i))
#define StopTimer(i) stop_timer((i))
#define ResetTimer(i) reset_timer((i))
#define PrintTimer(str,i) if(get_worker_id()==0)printf("%s : %f\n", (str), get_timer((i)));

namespace Pregel{

typedef int PartitionID;
typedef int VertexID;
typedef int WorkerID;

template<class T>
inline void insert_sorted(std::vector<T> & sorted, const T & new_v){
	typename std::vector<T>::iterator it=std::lower_bound(sorted.begin(), sorted.end(), new_v);
	if(it==sorted.end() || *it!=new_v)
		sorted.insert(it, new_v);
	else
		*it=new_v;
}

template<class T>
inline typename std::vector<T>::iterator find_sorted_by_id(std::vector<T> & sorted, int new_id){
	return std::lower_bound(sorted.begin(), sorted.end(), T(new_id));
}

};


#endif
