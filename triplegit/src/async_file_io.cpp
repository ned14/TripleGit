/* async_file_io
Provides a threadpool and asynchronous file i/o infrastructure based on Boost.ASIO, Boost.Iostreams and filesystem
(C) 2013 Niall Douglas http://www.nedprod.com/
File Created: Mar 2013
*/

#define MAX_NON_ASYNC_QUEUE_DEPTH 8
//#define USE_POSIX_ON_WIN32 // Useful for testing

#define _CRT_SECURE_NO_WARNINGS
#define _CRT_NONSTDC_DEPRECATE(a)

// This always compiles in input validation for this file only (the header file
// disables at the point of instance validation in release builds)
#ifndef TRIPLEGIT_NEVER_VALIDATE_INPUTS
#define TRIPLEGIT_VALIDATE_INPUTS 1
#endif

#include "../include/async_file_io.hpp"
#include "boost/smart_ptr/detail/spinlock.hpp"
#include "../../NiallsCPP11Utilities/ErrorHandling.hpp"
#include "../../NiallsCPP11Utilities/valgrind/memcheck.h"
#include "../../NiallsCPP11Utilities/valgrind/helgrind.h"
#include <mutex>

#include <fcntl.h>
#include <sys/stat.h>
#ifdef WIN32
#include <Windows.h>
// We also compile the posix compat layer for catching silly compile errors for POSIX
#include <io.h>
#include <direct.h>
#define posix_mkdir(path, mode) _wmkdir(path)
#define posix_rmdir _wrmdir
#define posix_stat _wstat64
#define stat _stat64
#define S_ISREG(m) ((m) & _S_IFREG)
#define S_ISDIR(m) ((m) & _S_IFDIR)
#define posix_open _wopen
#define posix_close _close
#define posix_unlink _wunlink
#define posix_fsync _commit
#define posix_ftruncate _chsize_s
#else
#include <sys/uio.h>
#include <limits.h>
#define posix_mkdir mkdir
#define posix_rmdir ::rmdir
#define posix_stat stat
#define posix_open open
#define posix_close ::close
#define posix_unlink unlink
#define posix_fsync fsync
#define posix_ftruncate ftruncate
#endif

// libstdc++ doesn't come with std::lock_guard
#define lock_guard boost::lock_guard

#if defined(_DEBUG) && 0
#define DEBUG_PRINTING 1
#ifdef WIN32
#define DEBUG_PRINT(...) \
	{ \
		char buffer[16384]; \
		sprintf(buffer, __VA_ARGS__); \
		OutputDebugStringA(buffer); \
	}
#else
#define DEBUG_PRINT(...) \
	{ \
		fprintf(stderr, __VA_ARGS__); \
	}
#endif
#else
#define DEBUG_PRINT(...)
#endif


#ifdef WIN32
#ifndef IOV_MAX
#define IOV_MAX 1024
#endif
struct iovec {
    void  *iov_base;    /* Starting address */
    size_t iov_len;     /* Number of bytes to transfer */
};
typedef ptrdiff_t ssize_t;
static boost::detail::spinlock preadwritelock;
ssize_t preadv(int fd, const struct iovec *iov, int iovcnt, triplegit::async_io::off_t offset)
{
	triplegit::async_io::off_t at=offset;
	ssize_t transferred;
	lock_guard<boost::detail::spinlock> lockh(preadwritelock);
	if(-1==_lseeki64(fd, offset, SEEK_SET)) return -1;
	for(; iovcnt; iov++, iovcnt--, at+=(triplegit::async_io::off_t) transferred)
		if(-1==(transferred=_read(fd, iov->iov_base, (unsigned) iov->iov_len))) return -1;
	return at-offset;
}
ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, triplegit::async_io::off_t offset)
{
	triplegit::async_io::off_t at=offset;
	ssize_t transferred;
	lock_guard<boost::detail::spinlock> lockh(preadwritelock);
	if(-1==_lseeki64(fd, offset, SEEK_SET)) return -1;
	for(; iovcnt; iov++, iovcnt--, at+=(triplegit::async_io::off_t) transferred)
		if(-1==(transferred=_write(fd, iov->iov_base, (unsigned) iov->iov_len))) return -1;
	return at-offset;
}
#endif


namespace triplegit { namespace async_io {

thread_pool &process_threadpool()
{
	// This is basically how many file i/o operations can occur at once
	// Obviously the kernel also has a limit
	static thread_pool ret(MAX_NON_ASYNC_QUEUE_DEPTH);
	return ret;
}

namespace detail {
#if defined(WIN32)
	struct async_io_handle_windows : public async_io_handle
	{
		std::shared_ptr<async_file_io_dispatcher_base> parent;
		std::unique_ptr<boost::asio::windows::random_access_handle> h;
		void *myid;
		bool has_been_added, autoflush;

		static HANDLE int_checkHandle(HANDLE h, const std::filesystem::path &path)
		{
			ERRHWINFN(INVALID_HANDLE_VALUE!=h, path);
			return h;
		}
		async_io_handle_windows(std::shared_ptr<async_file_io_dispatcher_base> _parent, const std::filesystem::path &path) : async_io_handle(_parent.get(), path), parent(_parent), myid(nullptr), has_been_added(false), autoflush(false) { }
		async_io_handle_windows(std::shared_ptr<async_file_io_dispatcher_base> _parent, const std::filesystem::path &path, bool _autoflush, HANDLE _h) : async_io_handle(_parent.get(), path), parent(_parent), h(new boost::asio::windows::random_access_handle(process_threadpool().io_service(), int_checkHandle(_h, path))), myid(_h), has_been_added(false), autoflush(_autoflush) { }
		virtual void *native_handle() const { return myid; }

		// You can't use shared_from_this() in a constructor so ...
		void do_add_io_handle_to_parent()
		{
			if(h)
			{
				parent->int_add_io_handle(myid, shared_from_this());
				has_been_added=true;
			}
		}
		~async_io_handle_windows()
		{
			DEBUG_PRINT("D %p\n", this);
			if(has_been_added)
				parent->int_del_io_handle(myid);
			if(h)
			{
				if(autoflush && write_count_since_fsync())
					ERRHWINFN(FlushFileBuffers(h->native_handle()), path());
				h->close();
			}
		}
	};
#endif
	struct async_io_handle_posix : public async_io_handle
	{
		std::shared_ptr<async_file_io_dispatcher_base> parent;
		std::shared_ptr<detail::async_io_handle> dirh;
		int fd;
		bool has_been_added, autoflush, has_ever_been_fsynced;

		async_io_handle_posix(std::shared_ptr<async_file_io_dispatcher_base> _parent, std::shared_ptr<detail::async_io_handle> _dirh, const std::filesystem::path &path, bool _autoflush, int _fd) : async_io_handle(_parent.get(), path), parent(_parent), dirh(_dirh), fd(_fd), has_been_added(false), autoflush(_autoflush),has_ever_been_fsynced(false)
		{
			if(fd!=-999)
				ERRHOSFN(fd, path);
		}
		virtual void *native_handle() const { return (void *)(size_t) fd; }

		// You can't use shared_from_this() in a constructor so ...
		void do_add_io_handle_to_parent()
		{
			parent->int_add_io_handle((void *)(size_t)fd, shared_from_this());
			has_been_added=true;
		}
		~async_io_handle_posix()
		{
			if(has_been_added)
				parent->int_del_io_handle((void *)(size_t)fd);
			if(fd>=0)
			{
				// Flush synchronously here? I guess ...
				if(autoflush && write_count_since_fsync())
					ERRHOSFN(posix_fsync(fd), path());
				ERRHOSFN(posix_close(fd), path());
				fd=-1;
			}
		}
	};

	enum class OpType
	{
		Unknown,
		UserCompletion,
		dir,
		rmdir,
		file,
		rmfile,
		sync,
		close,
		read,
		write,
		truncate,
		barrier,

		Last
	};
	static const char *optypes[]={
		"unknown",
		"UserCompletion",
		"dir",
		"rmdir",
		"file",
		"rmfile",
		"sync",
		"close",
		"read",
		"write",
		"truncate",
		"barrier"
	};
	static_assert(static_cast<size_t>(OpType::Last)==sizeof(optypes)/sizeof(*optypes), "You forgot to fix up the strings matching OpType");
	struct async_file_io_dispatcher_op
	{
		OpType optype;
		async_op_flags flags;
		std::shared_ptr<shared_future<std::shared_ptr<detail::async_io_handle>>> h;
		std::unique_ptr<promise<std::shared_ptr<detail::async_io_handle>>> detached_promise;
		typedef std::pair<size_t, std::function<std::shared_ptr<detail::async_io_handle> (std::shared_ptr<detail::async_io_handle>)>> completion_t;
		std::vector<completion_t> completions;
		async_file_io_dispatcher_op(OpType _optype, async_op_flags _flags, std::shared_ptr<shared_future<std::shared_ptr<detail::async_io_handle>>> _h)
			: optype(_optype), flags(_flags), h(_h) { }
		async_file_io_dispatcher_op(async_file_io_dispatcher_op &&o) : optype(o.optype), flags(std::move(o.flags)), h(std::move(o.h)),
			detached_promise(std::move(o.detached_promise)), completions(std::move(o.completions)) { }
	private:
		async_file_io_dispatcher_op(const async_file_io_dispatcher_op &o);
	};
	struct async_file_io_dispatcher_base_p
	{
		thread_pool &pool;
		file_flags flagsforce, flagsmask;

		typedef boost::detail::spinlock fdslock_t;
		typedef std::recursive_mutex opslock_t;
		fdslock_t fdslock; std::unordered_map<void *, std::weak_ptr<async_io_handle>> fds;
		opslock_t opslock; size_t monotoniccount; std::unordered_map<size_t, async_file_io_dispatcher_op> ops;

		async_file_io_dispatcher_base_p(thread_pool &_pool, file_flags _flagsforce, file_flags _flagsmask) : pool(_pool),
			flagsforce(_flagsforce), flagsmask(_flagsmask), monotoniccount(0)
		{
			// Boost's spinlock is so lightweight it has no constructor ...
			fdslock.unlock();
			ANNOTATE_RWLOCK_CREATE(&fdslock);
			ops.reserve(10000);
		}
		~async_file_io_dispatcher_base_p()
		{
			ANNOTATE_RWLOCK_DESTROY(&fdslock);
		}
	};
	class async_file_io_dispatcher_compat;
	class async_file_io_dispatcher_windows;
	class async_file_io_dispatcher_linux;
	class async_file_io_dispatcher_qnx;
	struct immediate_async_ops
	{
		typedef std::shared_ptr<detail::async_io_handle> rettype;
		typedef rettype retfuncttype();
		std::vector<packaged_task<retfuncttype>> toexecute;

		immediate_async_ops() { }
		// Returns a promise which is fulfilled when this is destructed
		future<rettype> enqueue(std::function<retfuncttype> f)
		{
			packaged_task<retfuncttype> t(std::move(f));
			toexecute.push_back(std::move(t));
			return toexecute.back().get_future();
		}
		~immediate_async_ops()
		{
			for(auto &i : toexecute)
				i();
		}
	private:
		immediate_async_ops(const immediate_async_ops &);
		immediate_async_ops &operator=(const immediate_async_ops &);
		immediate_async_ops(immediate_async_ops &&);
		immediate_async_ops &operator=(immediate_async_ops &&);
	};
}

async_file_io_dispatcher_base::async_file_io_dispatcher_base(thread_pool &threadpool, file_flags flagsforce, file_flags flagsmask) : p(new detail::async_file_io_dispatcher_base_p(threadpool, flagsforce, flagsmask))
{
}

async_file_io_dispatcher_base::~async_file_io_dispatcher_base()
{
	for(;;)
	{
		std::vector<std::shared_ptr<shared_future<std::shared_ptr<detail::async_io_handle>>>> outstanding;
		{
			lock_guard<detail::async_file_io_dispatcher_base_p::opslock_t> opslockh(p->opslock);
			if(!p->ops.empty())
			{
				outstanding.reserve(p->ops.size());
				for(auto &op : p->ops)
					if(op.second.h->valid())
						outstanding.push_back(op.second.h);
			}
		}
		if(outstanding.empty()) break;
		for(auto &op : outstanding)
			op->wait();
	}
	delete p;
}

void async_file_io_dispatcher_base::int_add_io_handle(void *key, std::shared_ptr<detail::async_io_handle> h)
{
	lock_guard<detail::async_file_io_dispatcher_base_p::fdslock_t> lockh(p->fdslock);
	ANNOTATE_RWLOCK_ACQUIRED(&p->fdslock, 1);
	p->fds.insert(make_pair(key, std::weak_ptr<detail::async_io_handle>(h)));
	ANNOTATE_RWLOCK_RELEASED(&p->fdslock, 1);
}

void async_file_io_dispatcher_base::int_del_io_handle(void *key)
{
	lock_guard<detail::async_file_io_dispatcher_base_p::fdslock_t> lockh(p->fdslock);
	ANNOTATE_RWLOCK_ACQUIRED(&p->fdslock, 1);
	p->fds.erase(key);
	ANNOTATE_RWLOCK_RELEASED(&p->fdslock, 1);
}

thread_pool &async_file_io_dispatcher_base::threadpool() const
{
	return p->pool;
}

file_flags async_file_io_dispatcher_base::fileflags(file_flags flags) const
{
	return (flags&~p->flagsmask)|p->flagsforce;
}

size_t async_file_io_dispatcher_base::wait_queue_depth() const
{
	lock_guard<detail::async_file_io_dispatcher_base_p::opslock_t> opslockh(p->opslock);
	return p->ops.size();
}

size_t async_file_io_dispatcher_base::count() const
{
	size_t ret;
	lock_guard<detail::async_file_io_dispatcher_base_p::fdslock_t> lockh(p->fdslock);
	ANNOTATE_RWLOCK_ACQUIRED(&p->fdslock, 1);
	ret=p->fds.size();
	ANNOTATE_RWLOCK_RELEASED(&p->fdslock, 1);
	return ret;
}

// Called in unknown thread
async_file_io_dispatcher_base::completion_returntype async_file_io_dispatcher_base::invoke_user_completion(size_t id, std::shared_ptr<detail::async_io_handle> h, std::function<async_file_io_dispatcher_base::completion_t> callback)
{
	return callback(id, h);
}

std::vector<async_io_op> async_file_io_dispatcher_base::completion(const std::vector<async_io_op> &ops, const std::vector<std::pair<async_op_flags, std::function<async_file_io_dispatcher_base::completion_t>>> &callbacks)
{
	if(!ops.empty() && ops.size()!=callbacks.size())
		throw std::runtime_error("The sequence of preconditions must either be empty or exactly the same length as callbacks.");
	std::vector<async_io_op> ret;
	ret.reserve(callbacks.size());
	std::vector<async_io_op>::const_iterator i;
	std::vector<std::pair<async_op_flags, std::function<async_file_io_dispatcher_base::completion_t>>>::const_iterator c;
	lock_guard<detail::async_file_io_dispatcher_base_p::opslock_t> opslockh(p->opslock);
	detail::immediate_async_ops immediates;
	if(ops.empty())
	{
		async_io_op empty;
		for(auto & c: callbacks)
			ret.push_back(chain_async_op(immediates, (int) detail::OpType::UserCompletion, empty, c.first, &async_file_io_dispatcher_base::invoke_user_completion, c.second));
	}
	else for(i=ops.begin(), c=callbacks.begin(); i!=ops.end() && c!=callbacks.end(); ++i, ++c)
			ret.push_back(chain_async_op(immediates, (int) detail::OpType::UserCompletion, *i, c->first, &async_file_io_dispatcher_base::invoke_user_completion, c->second));
	return ret;
}

// Called in unknown thread
void async_file_io_dispatcher_base::complete_async_op(size_t id, std::shared_ptr<detail::async_io_handle> h, exception_ptr e)
{
	lock_guard<detail::async_file_io_dispatcher_base_p::opslock_t> opslockh(p->opslock);
	detail::immediate_async_ops immediates;
	// Find me in ops, remove my completions and delete me from extant ops
	std::unordered_map<size_t, detail::async_file_io_dispatcher_op>::iterator it(p->ops.find(id));
	if(p->ops.end()==it)
	{
#ifndef NDEBUG
		std::vector<size_t> opsids;
		for(auto &i : p->ops)
			opsids.push_back(i.first);
		std::sort(opsids.begin(), opsids.end());
#endif
		throw std::runtime_error("Failed to find this operation in list of currently executing operations");
	}
	if(!it->second.completions.empty())
	{
		// Remove completions as we're about to modify p->ops which will invalidate it
		std::vector<detail::async_file_io_dispatcher_op::completion_t> completions(std::move(it->second.completions));
		for(auto &c : completions)
		{
			// Enqueue each completion
			it=p->ops.find(c.first);
			if(p->ops.end()==it)
				throw std::runtime_error("Failed to find this completion operation in list of currently executing operations");
			if(!!(it->second.flags & async_op_flags::ImmediateCompletion))
			{
				// If he was set up with a detached future, use that instead
				if(it->second.detached_promise)
				{
					*it->second.h=it->second.detached_promise->get_future();
					immediates.enqueue(std::bind(c.second, h));
				}
				else
					*it->second.h=immediates.enqueue(std::bind(c.second, h));
			}
			else
			{
				// If he was set up with a detached future, use that instead
				if(it->second.detached_promise)
				{
					*it->second.h=it->second.detached_promise->get_future();
					threadpool().enqueue(std::bind(c.second, h));
				}
				else
					*it->second.h=threadpool().enqueue(std::bind(c.second, h));
			}
			DEBUG_PRINT("C %u > %u %p\n", (unsigned) id, (unsigned) c.first, h.get());
		}
		// Restore it to my id
		it=p->ops.find(id);
		if(p->ops.end()==it)
		{
	#ifndef NDEBUG
			std::vector<size_t> opsids;
			for(auto &i : p->ops)
				opsids.push_back(i.first);
			std::sort(opsids.begin(), opsids.end());
	#endif
			throw std::runtime_error("Failed to find this operation in list of currently executing operations");
		}
	}
	if(it->second.detached_promise)
	{
		if(e)
			it->second.detached_promise->set_exception(e);
		else
			it->second.detached_promise->set_value(h);
	}
	p->ops.erase(it);
	DEBUG_PRINT("R %u %p\n", (unsigned) id, h.get());
}

// Called in unknown thread
template<class F, class... Args> std::shared_ptr<detail::async_io_handle> async_file_io_dispatcher_base::invoke_async_op_completions(size_t id, std::shared_ptr<detail::async_io_handle> h, completion_returntype (F::*f)(size_t, std::shared_ptr<detail::async_io_handle>, Args...), Args... args)
{
	try
	{
		completion_returntype ret((static_cast<F *>(this)->*f)(id, h, args...));
		// If boolean is false, reschedule completion notification setting it to ret.second, otherwise complete now
		if(ret.first)
		{
			complete_async_op(id, ret.second);
		}
		else
		{
			// Make sure this was set up for deferred completion
	#ifndef NDEBUG
			lock_guard<detail::async_file_io_dispatcher_base_p::opslock_t> opslockh(p->opslock);
			std::unordered_map<size_t, detail::async_file_io_dispatcher_op>::iterator it(p->ops.find(id));
			if(p->ops.end()==it)
			{
	#ifndef NDEBUG
				std::vector<size_t> opsids;
				for(auto &i : p->ops)
					opsids.push_back(i.first);
				std::sort(opsids.begin(), opsids.end());
	#endif
				throw std::runtime_error("Failed to find this operation in list of currently executing operations");
			}
			if(!it->second.detached_promise)
			{
				// If this trips, it means a completion handler tried to defer signalling
				// completion but it hadn't been set up with a detached future
				assert(0);
				std::terminate();
			}
	#endif
		}
		return ret.second;
	}
#ifdef _MSC_VER
	catch(const std::exception &)
	{
		exception_ptr e(async_io::make_exception_ptr(std::current_exception()));
		DEBUG_PRINT("E %u begin\n", (unsigned) id);
		complete_async_op(id, h, e);
		DEBUG_PRINT("E %u end\n", (unsigned) id);
		throw;
	}
	catch(const std::exception_ptr &)
#else
	catch(...)
#endif
	{
		exception_ptr e(async_io::make_exception_ptr(std::current_exception()));
		DEBUG_PRINT("E %u begin\n", (unsigned) id);
		complete_async_op(id, h, e);
		DEBUG_PRINT("E %u end\n", (unsigned) id);
		throw;
	}
}

// You MUST hold opslock before entry!
template<class F, class... Args> async_io_op async_file_io_dispatcher_base::chain_async_op(detail::immediate_async_ops &immediates, int optype, const async_io_op &precondition, async_op_flags flags, completion_returntype (F::*f)(size_t, std::shared_ptr<detail::async_io_handle>, Args...), Args... args)
{	
	size_t thisid=0;
	while(!(thisid=++p->monotoniccount));
#if 0 //ndef NDEBUG
	if(!p->ops.empty())
	{
		std::vector<size_t> opsids;
		for(auto &i : p->ops)
			opsids.push_back(i.first);
		std::sort(opsids.begin(), opsids.end());
		assert(thisid==opsids.back()+1);
	}
#endif
	// Wrap supplied implementation routine with a completion dispatcher
	auto wrapperf=&async_file_io_dispatcher_base::invoke_async_op_completions<F, Args...>;
	// Bind supplied implementation routine to this, unique id and any args they passed
	typename detail::async_file_io_dispatcher_op::completion_t boundf(std::make_pair(thisid, std::bind(wrapperf, this, thisid, std::placeholders::_1, f, args...)));
	// Make a new async_io_op ready for returning
	async_io_op ret(shared_from_this(), thisid);
	bool done=false;
	if(precondition.id)
	{
		// If still in flight, chain boundf to be executed when precondition completes
		auto dep(p->ops.find(precondition.id));
		if(p->ops.end()!=dep)
		{
			dep->second.completions.push_back(boundf);
			done=true;
		}
	}
	auto undep=NiallsCPP11Utilities::Undoer([done, this, precondition](){
		if(done)
		{
			auto dep(p->ops.find(precondition.id));
			dep->second.completions.pop_back();
		}
	});
	if(!done)
	{
		// Bind input handle now and queue immediately to next available thread worker
		std::shared_ptr<detail::async_io_handle> h;
		// Boost's shared_future has get() as non-const which is weird, because it doesn't
		// delete the data after retrieval.
		if(precondition.h->valid())
			h=const_cast<shared_future<std::shared_ptr<detail::async_io_handle>> &>(*precondition.h).get();
		else if(precondition.id)
		{
			// It should never happen that precondition.id is valid but removed from extant ops
			// which indicates it completed and yet h remains invalid
			assert(0);
			std::terminate();
		}
		if(!!(flags & async_op_flags::ImmediateCompletion))
			*ret.h=immediates.enqueue(std::bind(boundf.second, h)).share();
		else
			*ret.h=threadpool().enqueue(std::bind(boundf.second, h)).share();
	}
	auto opsit=p->ops.insert(std::make_pair(thisid, detail::async_file_io_dispatcher_op((detail::OpType) optype, flags, ret.h)));
	assert(opsit.second);
	DEBUG_PRINT("I %u < %u (%s)\n", (unsigned) thisid, (unsigned) precondition.id, detail::optypes[static_cast<int>(optype)]);
	auto unopsit=NiallsCPP11Utilities::Undoer([this, opsit, thisid](){
		p->ops.erase(opsit.first);
		DEBUG_PRINT("E R %u\n", (unsigned) thisid);
	});
	if(!!(flags & async_op_flags::DetachedFuture))
	{
		opsit.first->second.detached_promise.reset(new promise<std::shared_ptr<detail::async_io_handle>>);
		if(!done)
			*opsit.first->second.h=opsit.first->second.detached_promise->get_future();
	}
	unopsit.dismiss();
	undep.dismiss();
	return ret;
}
template<class F, class T> std::vector<async_io_op> async_file_io_dispatcher_base::chain_async_ops(int optype, const std::vector<async_io_op> &preconditions, const std::vector<T> &container, async_op_flags flags, completion_returntype (F::*f)(size_t, std::shared_ptr<detail::async_io_handle>, T))
{
	std::vector<async_io_op> ret;
	ret.reserve(container.size());
	assert(preconditions.size()==container.size());
	if(preconditions.size()!=container.size())
		throw std::runtime_error("preconditions size does not match size of ops data");
	lock_guard<detail::async_file_io_dispatcher_base_p::opslock_t> opslockh(p->opslock);
	detail::immediate_async_ops immediates;
	auto precondition_it=preconditions.cbegin();
	auto container_it=container.cbegin();
	for(; precondition_it!=preconditions.cend() && container_it!=container.cend(); ++precondition_it, ++container_it)
		ret.push_back(chain_async_op(immediates, optype, *precondition_it, flags, f, *container_it));
	return ret;
}
template<class F> std::vector<async_io_op> async_file_io_dispatcher_base::chain_async_ops(int optype, const std::vector<async_io_op> &container, async_op_flags flags, completion_returntype (F::*f)(size_t, std::shared_ptr<detail::async_io_handle>, async_io_op))
{
	std::vector<async_io_op> ret;
	ret.reserve(container.size());
	lock_guard<detail::async_file_io_dispatcher_base_p::opslock_t> opslockh(p->opslock);
	detail::immediate_async_ops immediates;
	for(auto &i : container)
		ret.push_back(chain_async_op(immediates, optype, i, flags, f, i));
	return ret;
}
template<class F> std::vector<async_io_op> async_file_io_dispatcher_base::chain_async_ops(int optype, const std::vector<async_path_op_req> &container, async_op_flags flags, completion_returntype (F::*f)(size_t, std::shared_ptr<detail::async_io_handle>, async_path_op_req))
{
	std::vector<async_io_op> ret;
	ret.reserve(container.size());
	lock_guard<detail::async_file_io_dispatcher_base_p::opslock_t> opslockh(p->opslock);
	detail::immediate_async_ops immediates;
	for(auto &i : container)
		ret.push_back(chain_async_op(immediates, optype, i.precondition, flags, f, i));
	return ret;
}
template<class F, class T> std::vector<async_io_op> async_file_io_dispatcher_base::chain_async_ops(int optype, const std::vector<async_data_op_req<T>> &container, async_op_flags flags, completion_returntype (F::*f)(size_t, std::shared_ptr<detail::async_io_handle>, async_data_op_req<T>))
{
	std::vector<async_io_op> ret;
	ret.reserve(container.size());
	lock_guard<detail::async_file_io_dispatcher_base_p::opslock_t> opslockh(p->opslock);
	detail::immediate_async_ops immediates;
	for(auto &i : container)
		ret.push_back(chain_async_op(immediates, optype, i.precondition, flags, f, i));
	return ret;
}

namespace detail
{
	struct barrier_count_completed_state
	{
		std::atomic<size_t> togo;
		std::vector<std::pair<size_t, std::shared_ptr<detail::async_io_handle>>> out;
		std::vector<std::shared_ptr<shared_future<std::shared_ptr<detail::async_io_handle>>>> outsharedstates;
		barrier_count_completed_state(const std::vector<async_io_op> &ops) : togo(ops.size()), out(ops.size())
		{
			outsharedstates.reserve(ops.size());
			for(auto &i : ops)
				outsharedstates.push_back(i.h);
		}
	};
}

/* This is extremely naughty ... you really shouldn't be using templates to hide implementation
types, but hey it works and is non-header so so what ...
*/
//template<class T> async_file_io_dispatcher_base::completion_returntype async_file_io_dispatcher_base::dobarrier(size_t id, std::shared_ptr<detail::async_io_handle> h, T state);
template<> async_file_io_dispatcher_base::completion_returntype async_file_io_dispatcher_base::dobarrier<std::pair<std::shared_ptr<detail::barrier_count_completed_state>, size_t>>(size_t id, std::shared_ptr<detail::async_io_handle> h, std::pair<std::shared_ptr<detail::barrier_count_completed_state>, size_t> state)
{
	size_t idx=state.second;
	state.first->out[idx]=std::make_pair(id, h); // This might look thread unsafe, but each idx is unique
	if(--state.first->togo)
		return std::make_pair(false, h);
	exception_ptr this_e(async_io::make_exception_ptr(std::current_exception()));
	// Last one just completed, so issue completions for everything in out except me
	detail::barrier_count_completed_state &s=*state.first;
	for(idx=0; idx<s.out.size(); idx++)
	{
		if(idx==state.second) continue;
		shared_future<std::shared_ptr<detail::async_io_handle>> *thisresult=state.first->outsharedstates[idx].get();
		if(thisresult->has_exception())
		{
			// This seems excessive but I don't see any other legal way to extract the exception ...
			try
			{
				thisresult->get();
			}
#ifdef _MSC_VER
			catch(const std::exception &)
			{
				exception_ptr e(async_io::make_exception_ptr(std::current_exception()));
				complete_async_op(s.out[idx].first, s.out[idx].second, e);
			}
			catch(const std::exception_ptr &)
#else
			catch(...)
#endif
			{
				exception_ptr e(async_io::make_exception_ptr(std::current_exception()));
				complete_async_op(s.out[idx].first, s.out[idx].second, e);
			}
		}
		else
			complete_async_op(s.out[idx].first, s.out[idx].second);
	}
	idx=state.second;
	// Am I being called because my precondition threw an exception so we're actually currently inside an exception catch?
	// If so then duplicate the same exception throw
	if(this_e)
		rethrow_exception(this_e);
	else
		return std::make_pair(true, h);
}

std::vector<async_io_op> async_file_io_dispatcher_base::barrier(const std::vector<async_io_op> &ops)
{
#if TRIPLEGIT_VALIDATE_INPUTS
		for(auto &i : ops)
			if(!i.validate())
				throw std::runtime_error("Inputs are invalid.");
#endif
	// Create a shared state for the completions to be attached to all the items we are waiting upon
	auto state(std::make_shared<detail::barrier_count_completed_state>(ops));
	std::vector<std::pair<std::shared_ptr<detail::barrier_count_completed_state>, size_t>> statev;
	statev.reserve(ops.size());
	size_t idx=0;
	for(auto &op : ops)
		statev.push_back(make_pair(state, idx++));
	return chain_async_ops((int) detail::OpType::barrier, ops, statev, async_op_flags::ImmediateCompletion|async_op_flags::DetachedFuture, &async_file_io_dispatcher_base::dobarrier<std::pair<std::shared_ptr<detail::barrier_count_completed_state>, size_t>>);
}


namespace detail {
#if defined(WIN32)
	class async_file_io_dispatcher_windows : public async_file_io_dispatcher_base
	{
		// Called in unknown thread
		completion_returntype dodir(size_t id, std::shared_ptr<detail::async_io_handle> _, async_path_op_req req)
		{
			BOOL ret=0;
			req.flags=fileflags(req.flags);
			if(!!(req.flags & (file_flags::Create|file_flags::CreateOnlyIfNotExist)))
			{
				ret=CreateDirectory(req.path.c_str(), NULL);
				if(!ret && ERROR_ALREADY_EXISTS==GetLastError())
				{
					// Ignore already exists unless we were asked otherwise
					if(!(req.flags & file_flags::CreateOnlyIfNotExist))
						ret=1;
				}
				req.flags=req.flags&~(file_flags::Create|file_flags::CreateOnlyIfNotExist);
			}
			DWORD attr=GetFileAttributes(req.path.c_str());
			if(INVALID_FILE_ATTRIBUTES!=attr && !(attr & FILE_ATTRIBUTE_DIRECTORY))
				throw std::runtime_error("Not a directory");
			if(!!(req.flags & file_flags::Read))
				return dofile(id, _, req);
			else
			{
				// Create empty handle so
				auto ret=std::make_shared<async_io_handle_windows>(shared_from_this(), req.path);
				return std::make_pair(true, ret);
			}
		}
		// Called in unknown thread
		completion_returntype dormdir(size_t id, std::shared_ptr<detail::async_io_handle> _, async_path_op_req req)
		{
			req.flags=fileflags(req.flags);
			ERRHWINFN(RemoveDirectory(req.path.c_str()), req.path);
			auto ret=std::make_shared<async_io_handle_windows>(shared_from_this(), req.path);
			return std::make_pair(true, ret);
		}
		// Called in unknown thread
		completion_returntype dofile(size_t id, std::shared_ptr<detail::async_io_handle>, async_path_op_req req)
		{
			DWORD access=0, creation=0, flags=FILE_ATTRIBUTE_NORMAL|FILE_FLAG_OVERLAPPED;
			req.flags=fileflags(req.flags);
			if(!!(req.flags & file_flags::Append)) access|=FILE_APPEND_DATA|SYNCHRONIZE;
			else
			{
				if(!!(req.flags & file_flags::Read)) access|=GENERIC_READ;
				if(!!(req.flags & file_flags::Write)) access|=GENERIC_WRITE;
			}
			if(!!(req.flags & file_flags::CreateOnlyIfNotExist)) creation|=CREATE_NEW;
			else if(!!(req.flags & file_flags::Create)) creation|=CREATE_ALWAYS;
			else if(!!(req.flags & file_flags::Truncate)) creation|=TRUNCATE_EXISTING;
			else creation|=OPEN_EXISTING;
			if(!!(req.flags & file_flags::WillBeSequentiallyAccessed))
				flags|=FILE_FLAG_SEQUENTIAL_SCAN;
			if(!!(req.flags & file_flags::OSDirect)) flags|=FILE_FLAG_NO_BUFFERING;
			if(!!(req.flags & file_flags::OSSync)) flags|=FILE_FLAG_WRITE_THROUGH;
			// If writing and autoflush and NOT synchronous, turn on autoflush
			auto ret=std::make_shared<async_io_handle_windows>(shared_from_this(), req.path, (file_flags::AutoFlush|file_flags::Write)==(req.flags & (file_flags::AutoFlush|file_flags::Write|file_flags::OSSync)),
				CreateFile(req.path.c_str(), access, FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
					NULL, creation, flags, NULL));
			static_cast<async_io_handle_windows *>(ret.get())->do_add_io_handle_to_parent();
			return std::make_pair(true, ret);
		}
		// Called in unknown thread
		completion_returntype dormfile(size_t id, std::shared_ptr<detail::async_io_handle> _, async_path_op_req req)
		{
			req.flags=fileflags(req.flags);
			ERRHWINFN(DeleteFile(req.path.c_str()), req.path);
			auto ret=std::make_shared<async_io_handle_windows>(shared_from_this(), req.path);
			return std::make_pair(true, ret);
		}
		// Called in unknown thread
		completion_returntype dosync(size_t id, std::shared_ptr<detail::async_io_handle> h, async_io_op)
		{
			async_io_handle_windows *p=static_cast<async_io_handle_windows *>(h.get());
			size_t bytestobesynced=p->write_count_since_fsync();
			assert(p);
			if(bytestobesynced)
				ERRHWINFN(FlushFileBuffers(p->h->native_handle()), p->path());
			p->byteswrittenatlastfsync+=(long) bytestobesynced;
			return std::make_pair(true, h);
		}
		// Called in unknown thread
		completion_returntype doclose(size_t id, std::shared_ptr<detail::async_io_handle> h, async_io_op)
		{
			async_io_handle_windows *p=static_cast<async_io_handle_windows *>(h.get());
			assert(p);
			// Windows doesn't provide an async fsync so do it synchronously
			if(p->autoflush && p->write_count_since_fsync())
				ERRHWINFN(FlushFileBuffers(p->h->native_handle()), p->path());
			p->h->close();
			p->h.reset();
			return std::make_pair(true, h);
		}
		// Called in unknown thread
		void boost_asio_completion_handler(bool is_write, size_t id, std::shared_ptr<detail::async_io_handle> h, const boost::system::error_code &ec, size_t bytes_transferred)
		{
			exception_ptr e;
			if(ec)
			{
				// boost::system::system_error makes no attempt to ask windows for what the error code means :(
				try
				{
					ERRGWINFN(ec.value(), h->path());
				}
				catch(...)
				{
					e=async_io::make_exception_ptr(current_exception());
				}
			}
			else
			{
				async_io_handle_windows *p=static_cast<async_io_handle_windows *>(h.get());
				if(is_write)
					p->byteswritten+=bytes_transferred;
				else
					p->bytesread+=bytes_transferred;
			}
			DEBUG_PRINT("H %u e=%u\n", (unsigned) id, (unsigned) ec.value());
			complete_async_op(id, h, e);
		}
		// Called in unknown thread
		completion_returntype doread(size_t id, std::shared_ptr<detail::async_io_handle> h, async_data_op_req<void> req)
		{
			async_io_handle_windows *p=static_cast<async_io_handle_windows *>(h.get());
			assert(p);
			DEBUG_PRINT("R %u %p (%c) @ %u, b=%u\n", (unsigned) id, h.get(), p->path().native().back(), (unsigned) req.where, (unsigned) req.buffers.size());
#ifdef DEBUG_PRINTING
			for(auto &b : req.buffers)
				DEBUG_PRINT("  R %u: %p %u\n", (unsigned) id, boost::asio::buffer_cast<const void *>(b), (unsigned) boost::asio::buffer_size(b));
#endif
			boost::asio::async_read_at(*p->h, req.where, req.buffers, boost::bind(&async_file_io_dispatcher_windows::boost_asio_completion_handler, this, false, id, h, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred));
			// Indicate we're not finished yet
			return std::make_pair(false, h);
		}
		// Called in unknown thread
		completion_returntype dowrite(size_t id, std::shared_ptr<detail::async_io_handle> h, async_data_op_req<const void> req)
		{
			async_io_handle_windows *p=static_cast<async_io_handle_windows *>(h.get());
			assert(p);
			DEBUG_PRINT("W %u %p (%c) @ %u, b=%u\n", (unsigned) id, h.get(), p->path().native().back(), (unsigned) req.where, (unsigned) req.buffers.size());
#ifdef DEBUG_PRINTING
			for(auto &b : req.buffers)
				DEBUG_PRINT("  W %u: %p %u\n", (unsigned) id, boost::asio::buffer_cast<const void *>(b), (unsigned) boost::asio::buffer_size(b));
#endif
			boost::asio::async_write_at(*p->h, req.where, req.buffers, boost::bind(&async_file_io_dispatcher_windows::boost_asio_completion_handler, this, true, id, h, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred));
			// Indicate we're not finished yet
			return std::make_pair(false, h);
		}
		// Called in unknown thread
		completion_returntype dotruncate(size_t id, std::shared_ptr<detail::async_io_handle> h, off_t _newsize)
		{
			async_io_handle_windows *p=static_cast<async_io_handle_windows *>(h.get());
			assert(p);
			DEBUG_PRINT("T %u %p (%c)\n", (unsigned) id, h.get(), p->path().native().back());
			// This is a bit tricky ... overlapped files ignore their file position except in this one
			// case, but clearly here we have a race condition. No choice but to rinse and repeat I guess.
			LARGE_INTEGER size={0}, newsize;
			newsize.QuadPart=_newsize;
			while(size.QuadPart!=newsize.QuadPart)
			{
				ERRHWINFN(SetFilePointerEx(p->h->native_handle(), newsize, NULL, FILE_BEGIN), p->path());
				ERRHWINFN(SetEndOfFile(p->h->native_handle()), p->path());
				ERRHWINFN(GetFileSizeEx(p->h->native_handle(), &size), p->path());
			}
			return std::make_pair(true, h);
		}

	public:
		async_file_io_dispatcher_windows(thread_pool &threadpool, file_flags flagsforce, file_flags flagsmask) : async_file_io_dispatcher_base(threadpool, flagsforce, flagsmask)
		{
		}

		virtual std::vector<async_io_op> dir(const std::vector<async_path_op_req> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::dir, reqs, async_op_flags::None, &async_file_io_dispatcher_windows::dodir);
		}
		virtual std::vector<async_io_op> rmdir(const std::vector<async_path_op_req> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::rmdir, reqs, async_op_flags::None, &async_file_io_dispatcher_windows::dormdir);
		}
		virtual std::vector<async_io_op> file(const std::vector<async_path_op_req> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::file, reqs, async_op_flags::None, &async_file_io_dispatcher_windows::dofile);
		}
		virtual std::vector<async_io_op> rmfile(const std::vector<async_path_op_req> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::rmfile, reqs, async_op_flags::None, &async_file_io_dispatcher_windows::dormfile);
		}
		virtual std::vector<async_io_op> sync(const std::vector<async_io_op> &ops)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : ops)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::sync, ops, async_op_flags::None, &async_file_io_dispatcher_windows::dosync);
		}
		virtual std::vector<async_io_op> close(const std::vector<async_io_op> &ops)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : ops)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::close, ops, async_op_flags::None, &async_file_io_dispatcher_windows::doclose);
		}
		virtual std::vector<async_io_op> read(const std::vector<async_data_op_req<void>> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::read, reqs, async_op_flags::DetachedFuture|async_op_flags::ImmediateCompletion, &async_file_io_dispatcher_windows::doread);
		}
		virtual std::vector<async_io_op> write(const std::vector<async_data_op_req<const void>> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::write, reqs, async_op_flags::DetachedFuture|async_op_flags::ImmediateCompletion, &async_file_io_dispatcher_windows::dowrite);
		}
		virtual std::vector<async_io_op> truncate(const std::vector<async_io_op> &ops, const std::vector<off_t> &sizes)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : ops)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::truncate, ops, sizes, async_op_flags::None, &async_file_io_dispatcher_windows::dotruncate);
		}
	};
#endif
	class async_file_io_dispatcher_compat : public async_file_io_dispatcher_base
	{
		// Keep an optional weak reference counted index of containing directories on POSIX
		typedef boost::detail::spinlock dircachelock_t;
		dircachelock_t dircachelock; std::unordered_map<std::filesystem::path, std::weak_ptr<async_io_handle>> dircache;
		std::shared_ptr<detail::async_io_handle> get_handle_to_containing_dir(const std::filesystem::path &path)
		{
			std::filesystem::path containingdir(path.parent_path());
			std::shared_ptr<detail::async_io_handle> dirh;
			lock_guard<dircachelock_t> dircachelockh(dircachelock);
			do
			{
				std::unordered_map<std::filesystem::path, std::weak_ptr<async_io_handle>>::iterator it=dircache.find(containingdir);
				if(dircache.end()==it || it->second.expired())
				{
					if(dircache.end()!=it) dircache.erase(it);
					dirh=std::make_shared<async_io_handle_posix>(std::shared_ptr<async_file_io_dispatcher_base>(), std::shared_ptr<detail::async_io_handle>(),
						containingdir, false, posix_open(containingdir.c_str(), O_RDONLY, 0x1b0/*660*/));
					auto _it=dircache.insert(std::make_pair(containingdir, std::weak_ptr<async_io_handle>(dirh)));
					return dirh;
				}
				else
					dirh=std::shared_ptr<async_io_handle>(it->second);
			} while(!dirh);
			return dirh;
		}

		// Called in unknown thread
		completion_returntype dodir(size_t id, std::shared_ptr<detail::async_io_handle> _, async_path_op_req req)
		{
			int ret=0;
			req.flags=fileflags(req.flags);
			if(!!(req.flags & (file_flags::Create|file_flags::CreateOnlyIfNotExist)))
			{
				ret=posix_mkdir(req.path.c_str(), 0x1f8/*770*/);
				if(-1==ret && EEXIST==errno)
				{
					// Ignore already exists unless we were asked otherwise
					if(!(req.flags & file_flags::CreateOnlyIfNotExist))
						ret=0;
				}
				req.flags=req.flags&~(file_flags::Create|file_flags::CreateOnlyIfNotExist);
			}

			struct stat s={0};
			ret=posix_stat(req.path.c_str(), &s);
			if(0==ret && !S_ISDIR(s.st_mode))
				throw std::runtime_error("Not a directory");
			if(file_flags::Read==(req.flags & file_flags::Read))
			{
				return dofile(id, _, req);
			}
			else
			{
				// Create dummy handle so
				std::shared_ptr<detail::async_io_handle> dirh;
#ifdef __linux__
				// Need to fsync the containing directory, otherwise the file isn't guaranteed to appear where we just created it
				if(!!(req.flags & (file_flags::Create|file_flags::CreateOnlyIfNotExist)) && !!(req.flags & (file_flags::AutoFlush|file_flags::OSSync)))
					req.flags=req.flags|file_flags::FastDirectoryEnumeration;
#endif
				if(!!(req.flags & file_flags::FastDirectoryEnumeration))
					dirh=get_handle_to_containing_dir(req.path);
				auto ret=std::make_shared<async_io_handle_posix>(shared_from_this(), dirh, req.path, false, -999);
#ifdef __linux__
				if(!!(req.flags & (file_flags::Create|file_flags::CreateOnlyIfNotExist)) && !!(req.flags & (file_flags::AutoFlush|file_flags::OSSync)))
					posix_fsync(static_cast<async_io_handle_posix *>(dirh.get())->fd);
#endif
				return std::make_pair(true, ret);
			}
		}
		// Called in unknown thread
		completion_returntype dormdir(size_t id, std::shared_ptr<detail::async_io_handle> _, async_path_op_req req)
		{
			req.flags=fileflags(req.flags);
			ERRHOSFN(posix_rmdir(req.path.c_str()), req.path);
			auto ret=std::make_shared<async_io_handle_posix>(shared_from_this(), std::shared_ptr<detail::async_io_handle>(), req.path, false, -999);
			return std::make_pair(true, ret);
		}
		// Called in unknown thread
		completion_returntype dofile(size_t id, std::shared_ptr<detail::async_io_handle>, async_path_op_req req)
		{
			int flags=0;
			std::shared_ptr<detail::async_io_handle> dirh;
			req.flags=fileflags(req.flags);
			if(!!(req.flags & file_flags::Read) && !!(req.flags & file_flags::Write)) flags|=O_RDWR;
			else if(!!(req.flags & file_flags::Read)) flags|=O_RDONLY;
			else if(!!(req.flags & file_flags::Write)) flags|=O_WRONLY;
			if(!!(req.flags & file_flags::Append)) flags|=O_APPEND;
			if(!!(req.flags & file_flags::Truncate)) flags|=O_TRUNC;
			if(!!(req.flags & file_flags::CreateOnlyIfNotExist)) flags|=O_EXCL|O_CREAT;
			else if(!!(req.flags & file_flags::Create)) flags|=O_CREAT;
#ifdef O_DIRECT
			if(!!(req.flags & file_flags::OSDirect)) flags|=O_DIRECT;
#endif
#ifdef O_SYNC
			if(!!(req.flags & file_flags::OSSync)) flags|=O_SYNC;
#endif
#ifdef __linux__
			// Need to fsync the containing directory, otherwise the file isn't guaranteed to appear where we just created it
			if((flags & O_CREAT) && !!(req.flags & (file_flags::AutoFlush|file_flags::OSSync)))
				req.flags=req.flags|file_flags::FastDirectoryEnumeration;
#endif
			if(!!(req.flags & file_flags::FastDirectoryEnumeration))
				dirh=get_handle_to_containing_dir(req.path);
			// If writing and autoflush and NOT synchronous, turn on autoflush
			auto ret=std::make_shared<async_io_handle_posix>(shared_from_this(), dirh, req.path, (file_flags::AutoFlush|file_flags::Write)==(req.flags & (file_flags::AutoFlush|file_flags::Write|file_flags::OSSync)),
				posix_open(req.path.c_str(), flags, 0x1b0/*660*/));
#ifdef __linux__
			if(!!(req.flags & (file_flags::Create|file_flags::CreateOnlyIfNotExist)) && !!(req.flags & (file_flags::AutoFlush|file_flags::OSSync)))
				posix_fsync(static_cast<async_io_handle_posix *>(dirh.get())->fd);
#endif
			static_cast<async_io_handle_posix *>(ret.get())->do_add_io_handle_to_parent();
			return std::make_pair(true, ret);
		}
		// Called in unknown thread
		completion_returntype dormfile(size_t id, std::shared_ptr<detail::async_io_handle> _, async_path_op_req req)
		{
			req.flags=fileflags(req.flags);
			ERRHOSFN(posix_unlink(req.path.c_str()), req.path);
			auto ret=std::make_shared<async_io_handle_posix>(shared_from_this(), std::shared_ptr<detail::async_io_handle>(), req.path, false, -999);
			return std::make_pair(true, ret);
		}
		// Called in unknown thread
		completion_returntype dosync(size_t id, std::shared_ptr<detail::async_io_handle> h, async_io_op)
		{
			async_io_handle_posix *p=static_cast<async_io_handle_posix *>(h.get());
			size_t bytestobesynced=p->write_count_since_fsync();
			if(bytestobesynced)
				ERRHOSFN(posix_fsync(p->fd), p->path());
			p->has_ever_been_fsynced=true;
			p->byteswrittenatlastfsync+=(long) bytestobesynced;
			return std::make_pair(true, h);
		}
		// Called in unknown thread
		completion_returntype doclose(size_t id, std::shared_ptr<detail::async_io_handle> h, async_io_op)
		{
			async_io_handle_posix *p=static_cast<async_io_handle_posix *>(h.get());
			if(p->autoflush && p->write_count_since_fsync())
				ERRHOSFN(posix_fsync(p->fd), p->path());
			ERRHOSFN(posix_close(p->fd), p->path());
			p->fd=-1;
			return std::make_pair(true, h);
		}
		// Called in unknown thread
		completion_returntype doread(size_t id, std::shared_ptr<detail::async_io_handle> h, async_data_op_req<void> req)
		{
			async_io_handle_posix *p=static_cast<async_io_handle_posix *>(h.get());
			ssize_t bytesread=0, bytestoread=0;
			iovec v;
			std::vector<iovec> vecs;
			vecs.reserve(req.buffers.size());
			DEBUG_PRINT("R %u %p (%c) @ %u, b=%u\n", (unsigned) id, h.get(), p->path().native().back(), (unsigned) req.where, (unsigned) req.buffers.size());
#ifdef DEBUG_PRINTING
			for(auto &b : req.buffers)
				DEBUG_PRINT("  R %u: %p %u\n", (unsigned) id, boost::asio::buffer_cast<const void *>(b), (unsigned) boost::asio::buffer_size(b));
#endif
			for(auto &b : req.buffers)
			{
				v.iov_base=boost::asio::buffer_cast<void *>(b);
				v.iov_len=boost::asio::buffer_size(b);
				bytestoread+=v.iov_len;
				vecs.push_back(v);
			}
			for(size_t n=0; n<vecs.size(); n+=IOV_MAX)
			{
				ssize_t _bytesread;
				ERRHOSFN((int) (_bytesread=preadv(p->fd, (&vecs.front())+n, std::min((int) (vecs.size()-n), IOV_MAX), req.where+bytesread)), p->path());
				p->bytesread+=_bytesread;
				bytesread+=_bytesread;
			}
			if(bytesread!=bytestoread)
				throw std::runtime_error("Failed to read all buffers");
			return std::make_pair(true, h);
		}
		// Called in unknown thread
		completion_returntype dowrite(size_t id, std::shared_ptr<detail::async_io_handle> h, async_data_op_req<const void> req)
		{
			async_io_handle_posix *p=static_cast<async_io_handle_posix *>(h.get());
			ssize_t byteswritten=0, bytestowrite=0;
			iovec v;
			std::vector<iovec> vecs;
			vecs.reserve(req.buffers.size());
			DEBUG_PRINT("W %u %p (%c) @ %u, b=%u\n", (unsigned) id, h.get(), p->path().native().back(), (unsigned) req.where, (unsigned) req.buffers.size());
#ifdef DEBUG_PRINTING
			for(auto &b : req.buffers)
				DEBUG_PRINT("  W %u: %p %u\n", (unsigned) id, boost::asio::buffer_cast<const void *>(b), (unsigned) boost::asio::buffer_size(b));
#endif
			for(auto &b : req.buffers)
			{
				v.iov_base=(void *) boost::asio::buffer_cast<const void *>(b);
				v.iov_len=boost::asio::buffer_size(b);
				bytestowrite+=v.iov_len;
				vecs.push_back(v);
			}
			for(size_t n=0; n<vecs.size(); n+=IOV_MAX)
			{
				ssize_t _byteswritten;
				ERRHOSFN((int) (_byteswritten=pwritev(p->fd, (&vecs.front())+n, std::min((int) (vecs.size()-n), IOV_MAX), req.where+byteswritten)), p->path());
				p->byteswritten+=_byteswritten;
				byteswritten+=_byteswritten;
			}
			if(byteswritten!=bytestowrite)
				throw std::runtime_error("Failed to write all buffers");
			return std::make_pair(true, h);
		}
		// Called in unknown thread
		completion_returntype dotruncate(size_t id, std::shared_ptr<detail::async_io_handle> h, off_t newsize)
		{
			async_io_handle_posix *p=static_cast<async_io_handle_posix *>(h.get());
			DEBUG_PRINT("T %u %p (%c)\n", (unsigned) id, h.get(), p->path().native().back());
			ERRHOSFN(posix_ftruncate(p->fd, newsize), p->path());
			return std::make_pair(true, h);
		}


	public:
		async_file_io_dispatcher_compat(thread_pool &threadpool, file_flags flagsforce, file_flags flagsmask) : async_file_io_dispatcher_base(threadpool, flagsforce, flagsmask)
		{
		}


		virtual std::vector<async_io_op> dir(const std::vector<async_path_op_req> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::dir, reqs, async_op_flags::None, &async_file_io_dispatcher_compat::dodir);
		}
		virtual std::vector<async_io_op> rmdir(const std::vector<async_path_op_req> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::rmdir, reqs, async_op_flags::None, &async_file_io_dispatcher_compat::dormdir);
		}
		virtual std::vector<async_io_op> file(const std::vector<async_path_op_req> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::file, reqs, async_op_flags::None, &async_file_io_dispatcher_compat::dofile);
		}
		virtual std::vector<async_io_op> rmfile(const std::vector<async_path_op_req> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::rmfile, reqs, async_op_flags::None, &async_file_io_dispatcher_compat::dormfile);
		}
		virtual std::vector<async_io_op> sync(const std::vector<async_io_op> &ops)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : ops)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::sync, ops, async_op_flags::None, &async_file_io_dispatcher_compat::dosync);
		}
		virtual std::vector<async_io_op> close(const std::vector<async_io_op> &ops)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : ops)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::close, ops, async_op_flags::None, &async_file_io_dispatcher_compat::doclose);
		}
		virtual std::vector<async_io_op> read(const std::vector<async_data_op_req<void>> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::read, reqs, async_op_flags::None, &async_file_io_dispatcher_compat::doread);
		}
		virtual std::vector<async_io_op> write(const std::vector<async_data_op_req<const void>> &reqs)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : reqs)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::write, reqs, async_op_flags::None, &async_file_io_dispatcher_compat::dowrite);
		}
		virtual std::vector<async_io_op> truncate(const std::vector<async_io_op> &ops, const std::vector<off_t> &sizes)
		{
#if TRIPLEGIT_VALIDATE_INPUTS
			for(auto &i : ops)
				if(!i.validate())
					throw std::runtime_error("Inputs are invalid.");
#endif
			return chain_async_ops((int) detail::OpType::truncate, ops, sizes, async_op_flags::None, &async_file_io_dispatcher_compat::dotruncate);
		}
	};
}

std::shared_ptr<async_file_io_dispatcher_base> async_file_io_dispatcher(thread_pool &threadpool, file_flags flagsforce, file_flags flagsmask)
{
#if defined(WIN32) && !defined(USE_POSIX_ON_WIN32)
	return std::make_shared<detail::async_file_io_dispatcher_windows>(threadpool, flagsforce, flagsmask);
#else
	return std::make_shared<detail::async_file_io_dispatcher_compat>(threadpool, flagsforce, flagsmask);
#endif
}

} } // namespace
