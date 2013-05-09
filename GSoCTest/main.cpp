/* GSoC programming test
*/
 
#include "../triplegit/include/async_file_io.hpp"
#include <iostream>
 
#include "boost/detail/lightweight_test.hpp"
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <utility>
 
using namespace triplegit::async_io;
using triplegit::async_io::future;
std::pair<std::vector<future<std::vector<std::filesystem::path>>>, std::vector<async_io_op>> async_file_io_dispatcher_base::enumerate(const std::vector<async_io_op> &ops, const std::vector<std::filesystem::path> &paths)
{
	typedef std::vector<std::filesystem::path> path_vector;
	typedef path_vector (*return_me_path)(const std::filesystem::path &);
	return_me_path callable=[](const std::filesystem::path& path) 
	{
		std::filesystem::directory_iterator it(path), end;
		std::vector<std::filesystem::path> retVector;
		while(it != end) 
				retVector.push_back((*it).path());
		return retVector;
	};
	std::vector<std::function<path_vector()>> callableVec;
	for(auto iter = paths.begin(); iter != paths.end(); ++iter)
	{
		callableVec.push_back(boost::bind(callable, boost::ref(*iter)));
	}
	return  this->call(ops, std::move(callableVec));
 
}
 
 
int main(int argc, const char *argv[])
{
 
	using namespace triplegit::async_io;
	using namespace std;
	using triplegit::async_io::future;
	auto dispatcher=async_file_io_dispatcher(process_threadpool(), file_flags::None);
	vector<filesystem::path> path_vector;
	std::cout << "\n\n100 directoreis creats, 10 fils in each directory opens, closes, and deletes:\n";
	typedef std::chrono::duration<double, ratio<1>> secs_type;
	auto mkdir(dispatcher->dir(async_path_op_req("testdir", file_flags::Create)));
	// Wait for six seconds to let filing system recover and prime SpeedStep
	auto begin=std::chrono::high_resolution_clock::now();
	while(std::chrono::duration_cast<secs_type>(std::chrono::high_resolution_clock::now()-begin).count()<6);
 
	//Start creating 100 directories
	begin=chrono::high_resolution_clock::now();
	std::vector<async_path_op_req> manydirreqs;
	manydirreqs.reserve(100);
	for(size_t n=0; n<100; n++)
	{
		manydirreqs.push_back(async_path_op_req(mkdir, "testdir/"+std::to_string(n), file_flags::Create));
		path_vector.push_back("testdir/"+std::to_string(n));
	}
 
	auto dirs(dispatcher->dir(manydirreqs));
	
	//Start creating 10 files in each dir
	std::vector<async_path_op_req> manyfilereqs;
	manyfilereqs.reserve(1000);
	for(size_t n=0; n<100; n++)
		for(size_t j=0;j<10;j++)
			manyfilereqs.push_back(async_path_op_req(mkdir, "testdir/"+std::to_string(n)+ "/" + std::to_string(j), file_flags::Create|file_flags::Write));
	//enumerate test	
	auto manyopenfiles(dispatcher->file(manyfilereqs));
	auto manyclosedfiles(dispatcher->close(manyopenfiles));
	auto it(manyclosedfiles.begin());
	for(auto &i : manyfilereqs)
		i.precondition=*it++;
	auto en(dispatcher->enumerate(manyclosedfiles, path_vector));
        auto it_2(en.second.begin());
	for(auto &i : manyfilereqs)
	   i.precondition=*it_2++;
	//Wait in each future
//	for(auto &i : en.first)	
//		i.wait();
	//Get result from future
// std::vector<filesystem::path> standard;
// for(int n = 0; n < 10; n++) standard.push_back(std::to_string(n));
// for(auto it = en.first.begin(); it != en.first.end; ++it)
// {
//    BOOST_TEST(*it.get() == standard);  
// }
//
    auto manydeletedfiles(dispatcher->rmfile(manyfilereqs));
    auto manydeleteddirs(dispatcher->rmdir(manydirreqs));
 
 
    //Wait all dirs to create
	when_all(dirs.begin(), dirs.end()).wait();
	auto dirsync=chrono::high_resolution_clock::now();
	// Wait for all files to create 
	when_all(manyopenfiles.begin(), manyopenfiles.end()).wait();
	auto openedsync=chrono::high_resolution_clock::now();
	//Wait for all files to close
	when_all(manyclosedfiles.begin(), manyclosedfiles.end()).wait();
	auto closedsync=chrono::high_resolution_clock::now();
 
	// Wait for all files to delete
	when_all(manydeletedfiles.begin(), manydeletedfiles.end()).wait();
	auto fdeletedsync=chrono::high_resolution_clock::now();
	//Wait all dirs to delete
	when_all(manydeleteddirs.begin(), manydeleteddirs.end()).wait();
	auto ddeletedsync=chrono::high_resolution_clock::now();
	auto end=ddeletedsync;
	auto rmdir(dispatcher->rmdir(async_path_op_req("testdir")));
 
	auto diff=chrono::duration_cast<secs_type>(end-begin);
	cout << "It took " << diff.count() << " secs to do all operations" << endl;
 
	diff=chrono::duration_cast<secs_type>(openedsync-begin);
	cout << "It took " << diff.count() << " secs to do " << manydirreqs.size()/diff.count() << " dir created per sec" << endl;
	cout << "It took " << diff.count() << " secs to do " << manyfilereqs.size()/diff.count() << " file created per sec" << endl;
	cout << "It took " << diff.count() << " secs to do " << manydeletedfiles.size()/diff.count() << " file deleted per sec" << endl;
 
	// Fetch any outstanding error
	rmdir.h.get();
    return 0;
}
