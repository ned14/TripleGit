/* GSoC programming test
*/

#include "../triplegit/include/async_file_io.hpp"
#include <iostream>
#include "boost/detail/lightweight_test.hpp"

/*	WILL_COMPLETE is a macro used to switch between a version that will complete, while failing to meet the test guidlines,
	and one that meets the test guidlines and will not complete due to an access violation, but follows	the guidelines exactly.
	
	WILL_COMPLETE will enumerate the contents of a single folder, "testdir" with 100 subfolders, and pass a set of 	unit tests

	The other version will try to enumerate each of the 100 subfolders in "testdir" asynchonously, and get a segmentation fault
	As Niall has pointed out to me, I'm likely trying to access a stack local that is out of scope, but so far	I cannot identify it.	*/
//#define WILL_COMPLETE

	static std::vector<std::filesystem::path> doEnum(std::filesystem::path p) {
		std::vector<std::filesystem::path> vec;
		for( auto it = std::filesystem::directory_iterator(p); it != std::filesystem::directory_iterator(); ++it)
			vec.push_back((*it).path().filename());
		return vec;
	};

// the definition of enumerate() is in async_file_io.hpp, so either update your copy, or use mine
/** 
	implementation of enumerate()
	@param ops a vector of precondions for the directory supplied from paths to be enumerated
  	@param paths a vector of paths(file system locations) to be enumerated, each path should be a directory
	@return a pair of vectors, one is a vector of futures, where each future is a vector of paths corresponding to the enumeration of a path from paths\
		the second vector is a vector of async_io_ops corresponding to the completion of one of the enumerations	*/
std::pair<std::vector<triplegit::async_io::future<std::vector<std::filesystem::path>>>, std::vector<triplegit::async_io::async_io_op>> triplegit::async_io::async_file_io_dispatcher_base::
	enumerate(const std::vector<async_io_op> &ops, const std::vector<std::filesystem::path> &paths)
{
	assert(ops.size() == paths.size());
	std::vector<std::function<std::vector<std::filesystem::path>()> > callbacks;
	callbacks.reserve(ops.size());
	for(const auto &item : paths)
		callbacks.push_back(std::bind(doEnum, item));

	//for(auto i = 0; i < paths.size(); ++i)
		//callbacks.push_back(std::bind(doEnum, paths[i]));		

	return call(ops, callbacks);
}

/**
  * a simple function to test if two vectors have the same items
  * complexity is O(N^2), so quite poor, but cannot sort paths, so no binary search
  * @param v1 a vector who's items are to be compared with v2's items
  * @param v2 a vector who's items are to be compared with v1's items
  * @return returns true if the v1 and v2 contain equal sets, irrespective of order
*/
template <class T>
bool equal_vects(const std::vector<T>& v1, const std::vector<T>& v2)
{
	// paths don't like to be sorted, so no easy compares
	//std::sort(v1.begin(), v1.end()); 
	//std::sort(v2.begin(), v2.end());
	if(v1.size() != v2.size())
		return false;


	// need a comparision with less complexity, could put in std::unordered_map, but is that any better? trading space for time...
	for(auto item :v1)
	{
		if(std::find(v2.begin(), v2.end(), item) == v2.end()) 
			return false;
	}
	return true;
}


int main(int argc, const char *argv[])
{
#pragma region namespaces
	using namespace triplegit::async_io;
	using namespace std;
	using triplegit::async_io::future;
#pragma endregion

#pragma region directory setup
	// set up dispatcher
	auto dispatcher=triplegit::async_io::async_file_io_dispatcher(triplegit::async_io::process_threadpool(), triplegit::async_io::file_flags::None);
	
	// create a test directory-- testdir
	auto mkdir(dispatcher->dir(async_path_op_req("testdir", file_flags::Create)));


	//create 100 sub directories to testdir, numbered 0-99
	std::vector<triplegit::async_io::async_path_op_req> many_dir_reqs;
	many_dir_reqs.reserve(100);
	for(size_t n=0; n<100; n++)
		many_dir_reqs.push_back(async_path_op_req(mkdir, "testdir/"+std::to_string(n), file_flags::Create));
	auto many_dirs(dispatcher->dir(many_dir_reqs));

	// create 10 files per folder numbered 1-10
	std::vector<triplegit::async_io::async_path_op_req> manyfilereqs;
	manyfilereqs.reserve(1000);
	for(size_t n = 0; n < 100; ++n)
	{
		auto precondition = many_dirs[n];
		for(size_t m=0; m < 10; ++m)
			manyfilereqs.push_back(async_path_op_req(precondition,"testdir/"+std::to_string(n) + "/" + std::to_string(m), 
			file_flags::Create|file_flags::Write));
	}
	auto manyopenfiles(dispatcher->file(manyfilereqs));// create files

	// Close each of those 1000 files as they are opened
	auto manyclosedfiles(dispatcher->close(manyopenfiles));
		
	when_all(manyclosedfiles.begin(), manyclosedfiles.end()).wait();//when all files are closed
#pragma endregion

#pragma region setup paths
	// create vector of paths
	std::vector<std::filesystem::path> paths;
	paths.reserve(many_dir_reqs.size());
	for(auto &i : many_dir_reqs)
		paths.push_back(i.path); // this is safe, because these paths are already created, and are not asynchronus => paths should be valid

	//print out paths to be sure they are correct --Testing 
	/*for(auto p : paths)
	{
		//cout << p.id << endl;
		//cout << p << endl;
		for( auto it = std::filesystem::directory_iterator(p); it != std::filesystem::directory_iterator(); ++it)
			cout << (*it).path() <<endl;
	}//*/
	
#pragma endregion


#pragma region enumeration of a directory

#ifdef WILL_COMPLETE
	std::vector<std::filesystem::path> path_testdir;  
	path_testdir.push_back(  async_path_op_req("testdir", file_flags::Create).path);
	std::vector<triplegit::async_io::async_io_op> op_testdir;
	op_testdir.push_back(many_dirs.back());
	
	// ADITIONAL SUB FOLDER ADDIONS
/*	// for testing variable length vectors of ops and paths
	for (auto i = 0; i < 1; ++i)
	{		
		path_testdir.push_back(paths[i]);
		op_testdir.push_back(manyclosedfiles.back());
	}//*/
	auto output(dispatcher->enumerate(op_testdir, path_testdir));
#else
	// set preconditions
	std::vector<triplegit::async_io::async_io_op> enum_op; // preconditions for enumerate
	enum_op.reserve(paths.size());							//should be the same size as the number of paths we give it
	//auto precondition(triplegit::async_io::async_io_op(manyclosedfiles.back()));	// the precondition is that all files be closed
	for(auto m: paths)
		enum_op.push_back(manyclosedfiles.back());	//*/							//put the precondition in the vector 

	auto output(dispatcher->enumerate(enum_op, paths));
#endif
	
	
	
#pragma endregion 


#pragma region clean up directories and files
	// Delete each of the files once they are closed
	for(auto i = 0; i < output.second.size(); ++i)
		for(auto j = 0; j < 1000/output.second.size(); ++j)
			manyfilereqs[10*i+j].precondition = output.second[i];
	auto manydeletedfiles(dispatcher->rmfile(manyfilereqs));

	
	// I use prereqs for this portion, but I think when_all is more clear until we have Barrier()
 	//when_all(manydeletedfiles.begin(), manydeletedfiles.end()).wait();
	for(auto &item : many_dir_reqs)
		item.precondition = manydeletedfiles.back();
	auto many_deleted_dirs(dispatcher->rmdir(many_dir_reqs));

	when_all(many_deleted_dirs.begin(), many_deleted_dirs.end()).wait(); //turned on <-- see comment below

	//precondition doesn't work here . . . is there a mistake in my logic?? Does many_deleted_dirs.back() not ensure
	// that all the directories have finished being removed?
	auto rmdir(dispatcher->rmdir(async_path_op_req(many_deleted_dirs.back(),"testdir")));//*/

#pragma endregion
	rmdir.h.get();

	
#pragma region tests
	// set up vector of filenames to compare
std::vector<std::filesystem::path> test_paths;

#ifdef WILL_COMPLETE
test_paths.reserve(100);// get memory if we need it
for( int num = 0 ; num < 100; ++num)	
	test_paths.push_back(std::filesystem::path(std::to_string(num)));
#else
test_paths.reserve(10);// get memory if we need it
for( int num = 0 ; num < 10; ++num)	
	test_paths.push_back(std::filesystem::path(std::to_string(num)));
#endif

	// This section tests to see if each vector of paths has leafs  labeled 0-10 or 0-100, depending on if WILL_COMPLETE macro is defined or not
	int i = 0;
	for(auto &fut : output.first)  //fut is a future holding a vector of paths
	{
		cout << "vector " << i << endl;
		
		auto temp = fut.get();

		// general formula for testing testdir and its subfolders together
		/*test_paths.clear();
		test_paths.reserve(temp.size());// get memory if we need it
		for( int num = 0 ; num < temp.size(); ++num)	
			test_paths.push_back(std::filesystem::path(std::to_string(i))); */

		BOOST_TEST(equal_vects(test_paths, temp ));
		for(auto item: temp)
			cout << item << endl;	
		++i;
	}//*/
#pragma endregion

	//system("pause");
    return boost::report_errors();
}
