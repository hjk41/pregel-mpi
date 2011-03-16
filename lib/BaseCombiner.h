#ifndef BASECOMBINER_H
#define BASECOMBINER_H

namespace Pregel{

//=======================
// Combiner
// combines new_msg with old one
//
template<class MessageT>
class BaseCombiner{
public:
	typedef MessageT MessageType;
	inline void combine(MessageT & old, const MessageT & new_msg){};
};


};	// namespace Pregel
#endif	// ifdef BASECOMBINER_H
