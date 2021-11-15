#pragma once

#include <stack>
#include <memory>
#include <unordered_map>

namespace clickhouse {

/** Collection of owned OutputStream or InputStream instances.
 *  Simplifies building chains or trees of streams, like:
 *
 *  A => B => C => F
 *            ^
 *            /
 *  D ====> E
 *
 *  Streams are destroyed in LIFO order, allowing proper flushing of internal buffers.
 */
template <typename StreamType>
class Streams
{
public:
    Streams() = default;
    Streams(Streams&&) = default;
    Streams& operator=(Streams&&) = default;

    ~Streams() {
        while (!streams_.empty()) {
            streams_.pop();
        }
    }

    template <typename ConcreteStreamType>
    inline ConcreteStreamType * Add(std::unique_ptr<ConcreteStreamType> && stream) {
        auto ret = stream.get();
        streams_.emplace(std::move(stream));
        return ret;
    }

    template <typename ConcreteStreamType, typename ...Args>
    inline ConcreteStreamType * AddNew(Args&&... args) {
        return Add(std::make_unique<ConcreteStreamType>(std::forward<Args>(args)...));
    }

    inline StreamType * Top() const {
        return streams_.top().get();
    }

private:
    std::stack<std::unique_ptr<StreamType>> streams_;
};

class OutputStream;
class InputStream;

using OutputStreams = Streams<OutputStream>;
using InputStreams = Streams<InputStream>;

}
