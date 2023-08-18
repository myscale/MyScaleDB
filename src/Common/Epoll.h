#pragma once
#if defined(OS_LINUX)

#include <sys/epoll.h>
#include <vector>
#include <functional>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>

namespace DB
{

using AsyncCallback = std::function<void(int, Poco::Timespan, const std::string &)>;

class Epoll
{
public:
    Epoll();

    Epoll(const Epoll &) = delete;
    Epoll & operator=(const Epoll &) = delete;

    Epoll & operator=(Epoll && other) noexcept;
    Epoll(Epoll && other) noexcept;

    /// Add new file descriptor to epoll. If ptr set to nullptr, epoll_event.data.fd = fd,
    /// otherwise epoll_event.data.ptr = ptr.
    void add(int fd, void * ptr = nullptr);

    /// Remove file descriptor to epoll.
    void remove(int fd);

    /// Get events from epoll. Events are written in events_out, this function returns an amount of
    /// ready events. The timeout argument specifies the number of milliseconds to wait for ready
    /// events. Timeout of -1 causes epoll_wait() to block indefinitely, while specifying a timeout
    /// equal to zero will return immediately, even if no events are available.
    size_t getManyReady(int max_events, epoll_event * events_out, int timeout) const;

    int getFileDescriptor() const { return epoll_fd; }

    int size() const { return events_count; }

    bool empty() const { return events_count == 0; }

    const std::string & getDescription() const { return fd_description; }

    ~Epoll();

private:
    int epoll_fd;
    std::atomic<int> events_count;
    const std::string fd_description = "epoll";
};

}
#endif
