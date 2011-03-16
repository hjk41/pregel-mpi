#ifndef PREGEL_TIME_H
#define PREGEL_TIME_H

#include <sys/time.h>
#include <stdio.h>

namespace Pregel{

double get_current_time(){
	timeval t;
	gettimeofday(&t,0);
	return (double)t.tv_sec+(double)t.tv_usec/1000000;
}

const int N_Timers=5;
static double _timers[N_Timers];	// timers
static double _acc_time[N_Timers];	// accumulated time
void init_timers(){
	for(int i=0;i<N_Timers;i++){
		_acc_time[i]=0;
	}
}

enum TIMERS{WORKER_TIMER=0, MARSHALL_TIMER=1, TRANSFER_TIMER=2, COMMUNICATION_TIMER=3};

void start_timer(int i){
	_timers[i]=get_current_time();
}
void reset_timer(int i){
	_timers[i]=get_current_time();
	_acc_time[i]=0;
}
void stop_timer(int i){
	double t=get_current_time();
	_acc_time[i]+=t-_timers[i];
}
double get_timer(int i){
	return _acc_time[i];
}

};

#endif
