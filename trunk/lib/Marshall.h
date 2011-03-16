#ifndef MARSHALL_H
#define MARSHALL_H

#include <vector>
#include <assert.h>
#include <set>


namespace Pregel{

class Marshall{
private:
	std::vector<char> buf;
public:
	const char * get_buf(){
		return &buf[0];
	}
	size_t size(){
		return buf.size();
	}
	void raw_byte(char c){
		buf.push_back(c);
	}
	void raw_bytes(const void * ptr, int size){
		buf.insert(buf.end(), (const char*)ptr, (const char*)ptr+size);
	}
};

Marshall & operator<<(Marshall & m, size_t i){
	m.raw_bytes(&i, sizeof(size_t));
	return m;
}
Marshall & operator<<(Marshall & m, int i){
	m.raw_bytes(&i, sizeof(int));
	return m;
}
Marshall & operator<<(Marshall & m, double i){
	m.raw_bytes(&i, sizeof(double));
	return m;
}
/*
Marshall & operator<<(Marshall & m, unsigned int i){
	m.raw_bytes(&i, sizeof(unsigned int));
	return m;
}*/
Marshall & operator<<(Marshall & m, char c){
	m.raw_byte(c);
	return m;
}

template<class T>
Marshall & operator<<(Marshall & m, const T * p){
	return m<<*p;
}

template<class T>
Marshall & operator<<(Marshall & m, const std::vector<T> & v){
	m<<v.size();
	for(typename std::vector<T>::const_iterator it=v.begin(); it!=v.end(); ++it){
		m<<*it;
	}
	return m;
}

template<>
Marshall & operator<<(Marshall & m, const std::vector<int> & v){
	m<<v.size();
	m.raw_bytes(&v[0], v.size()*sizeof(int));
	return m;
}

template<>
Marshall & operator<<(Marshall & m, const std::vector<double> & v){
	m<<v.size();
	m.raw_bytes(&v[0], v.size()*sizeof(double));
	return m;
}

template<class T>
Marshall & operator<<(Marshall & m, const std::set<T> & v){
	m<<v.size();
	for(typename std::set<T>::const_iterator it=v.begin(); it!=v.end(); ++it){
		m<<*it;
	}
	return m;
}

class Unmarshall{
private:
	char * buf;
	size_t size;
	size_t index;
public:
	Unmarshall(char * b, size_t s):buf(b),size(s),index(0){};
	Unmarshall(const Unmarshall & r){
		*this=r;
	}
	Unmarshall & operator=(const Unmarshall & r){
		Unmarshall & rhs=const_cast<Unmarshall &>(r);
		buf=rhs.buf;
		size=rhs.size;
		index=rhs.index;
		rhs.buf=NULL;
		rhs.size=0;
		rhs.index=0;
		return *this;
	}
	~Unmarshall(){delete[] buf;}
	
	char raw_byte(){
		return buf[index++];
	}
	void * raw_bytes(unsigned int n_bytes){
		WorkerLog("Unmarshall: index=%d, n_bytes=%d, size=%d\n",index,n_bytes,size);
		assert(index+n_bytes<=size);
		char * ret=buf+index;
		index+=n_bytes;
		return ret;
	}
};

Unmarshall & operator>>(Unmarshall & m, size_t & i){
	i=*(size_t*)m.raw_bytes(sizeof(size_t));
	return m;
}
Unmarshall & operator>>(Unmarshall & m, int & i){
	i=*(int*)m.raw_bytes(sizeof(int));
	return m;
}
Unmarshall & operator>>(Unmarshall & m, double & i){
	i=*(double*)m.raw_bytes(sizeof(double));
	return m;
}
Unmarshall & operator>>(Unmarshall & m, char & c){
	c=m.raw_byte();
	return m;
}

template<class T>
Unmarshall & operator>>(Unmarshall & m, T * & p){
	p=new T;
	return m>>(*p);
}
template<class T>
Unmarshall & operator>>(Unmarshall & m, std::vector<T> & v){
	size_t size;
	m>>size;
	WorkerLog("Unmarshalled vector size: %d\n",size);
	v.resize(size);
	for(typename std::vector<T>::iterator it=v.begin(); it!=v.end(); ++it){
		m>>*it;
	}
	return m;
}
template<>
Unmarshall & operator>>(Unmarshall & m, std::vector<int> & v){
	size_t size;
	m>>size;
	WorkerLog("Unmarshalled vector size: %d\n",size);
	v.resize(size);
	int * data=(int*)m.raw_bytes(sizeof(int)*size);
	v.assign(data,data+size);
	return m;
}
template<>
Unmarshall & operator>>(Unmarshall & m, std::vector<double> & v){
	size_t size;
	m>>size;
	WorkerLog("Unmarshalled vector size: %d\n",size);
	v.resize(size);
	double * data=(double*)m.raw_bytes(sizeof(double)*size);
	v.assign(data,data+size);
	return m;
}
template<class T>
Unmarshall & operator>>(Unmarshall & m, std::set<T> & v){
	size_t size;
	m>>size;
	for(size_t i=0;i<size;i++){
		T tmp;
		m>>tmp;
		v.insert(v.end(), tmp);
	}
	return m;
}


};

#endif
