#ifndef BASEGRAPHDUMPER_H
#define BASEGRAPHDUMPER_H

#include <vector>
#include <string>
#include <fstream>

namespace Pregel{

template<class VertexT>
class BaseGraphDumper{
public:
	void dump_partition(const std::string & output_file, const std::vector<VertexT> & vertexes){
	}

	inline PartitionID id(){return _id;}
	inline void set_id(PartitionID id){_id=id;};
private:
	PartitionID _id;
};

};

#endif
