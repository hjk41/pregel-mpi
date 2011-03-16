#ifndef COMMUNICATION_H
#define COMMUNICATION_H

#include <mpi.h>
#include <vector>
#include <algorithm>
#include <assert.h>
#include <stdio.h>
#include "Time.h"
#include "Marshall.h"
#include "Defines.h"

namespace Pregel{
//============================================
// functions on communication

static int _my_rank, _num_processors;
int get_worker_id(){
	return _my_rank;
}

int get_num_workers(){
	return _num_processors;
}

void init_workers(int * argc, char *** argv ){
	MPI_Init(argc,argv);
	MPI_Comm_size(MPI_COMM_WORLD, &_num_processors);
	MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
}

void worker_finalize(){
	MPI_Finalize();
}

void worker_barrier(){
	MPI_Barrier(MPI_COMM_WORLD);
}

int all_sum(int my_copy){
	int tmp;
	MPI_Allreduce((char *)(void*)&my_copy, (char *)(void*)&tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
	return tmp;
}

void pregel_send(const void * buf, int size, int dst){
	MPI_Send((char*)buf, size, MPI_CHAR, dst, 0, MPI_COMM_WORLD);
}

void pregel_recv(void * buf, int size, int src){
	MPI_Recv(static_cast<char*>(buf), size, MPI_CHAR, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

void send_marshall(Marshall & m, int dst){
	size_t size=m.size();
	pregel_send(&size, sizeof(size_t), dst);
	pregel_send(m.get_buf(), m.size(), dst);
	WorkerLog("worker %d sending %d bytes to %d\n", get_worker_id(), m.size(), dst);
}

Unmarshall recv_unmarshall(int src){
	size_t size;
	pregel_recv(&size, sizeof(size_t), src);
	char * buf=new char[size];
	pregel_recv(buf, size, src);
	WorkerLog("worker %d received %d bytes from %d\n", get_worker_id(), size, src);
	return Unmarshall(buf, size);
}

template<class T>
void send_data(const T & data, int dst){
	Marshall m;
	m<<data;
	send_marshall(m,dst);
}

template<class T>
T recv_data(int src){
	Unmarshall um=recv_unmarshall(src);
	T data;
	um>>data;
	return data;
}

// ************************************************************
// CARE! special code for RCCE emulator mode
//
// ************************************************************
#ifdef MULTITHREAD_EMU
void ** volatile global_message_buffer=NULL;
inline void set_message_buffer(void * mb){global_message_buffer[get_worker_id()]=mb;}
inline void* get_message_buffer(){return global_message_buffer[get_worker_id()];}
//-----------------------
// graph buffer
void ** volatile global_graph_buffer=NULL;
inline void set_graph_buffer(void * gb){global_graph_buffer[get_worker_id()]=gb;}
inline void* get_graph_buffer(){return global_graph_buffer[get_worker_id()];}
//----------------------
// global data
int * volatile global_halt_count=NULL;
inline void set_halt_count(int i){global_halt_count[get_worker_id()]=i;}
inline int get_halt_count(){return global_halt_count[get_worker_id()];}
inline void vote_for_halt(){global_halt_count[get_worker_id()]++;}
int global_step_num;
inline int step_num(){return global_step_num;}

void init_pregel(int argc, char ** argv){
	init_workers(&argc,&argv);
	int n=get_num_workers();
	if(get_worker_id()==0){
		global_message_buffer=new void*[n];
		global_graph_buffer=new void*[n];
		global_halt_count=new int[n];
	}
	worker_barrier();
}

#else
void * global_message_buffer=NULL;
inline void set_message_buffer(void * mb){global_message_buffer=mb;}
inline void* get_message_buffer(){return global_message_buffer;}
//-----------------------
// graph buffer
void * global_graph_buffer=NULL;
inline void set_graph_buffer(void * gb){global_graph_buffer=gb;}
inline void* get_graph_buffer(){return global_graph_buffer;}
//----------------------
// global data
int global_halt_count=0;
inline void set_halt_count(int i){global_halt_count=i;}
inline int get_halt_count(){return global_halt_count;}
inline void vote_for_halt(){global_halt_count++;}
int global_step_num;
inline int step_num(){return global_step_num;}

void * _global_aggregator=NULL;
void _set_aggregator(void * ptr){_global_aggregator=ptr;};
inline void * _get_aggregator(){return _global_aggregator;}

void init_pregel(int argc, char ** argv){
        init_workers(&argc,&argv);
        init_timers();
        worker_barrier();
}
#endif
};


#endif
